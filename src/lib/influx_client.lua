--- InfluxDB connection management for the Data Logger driver.
---
--- Manages connection configuration, status tracking, health checks, and
--- provides URL/header building for InfluxDB API requests.

local log = require("lib.logging")
local http = require("lib.http")
local constants = require("constants")

---------------------------------------------------------------------------
-- Module
---------------------------------------------------------------------------

--- @class InfluxConfig
--- @field url string InfluxDB server URL.
--- @field token string API authentication token.
--- @field database string Database/bucket name.
--- @field precision string Write precision (ns, us, ms, s).
--- @field writeInterval number Default write interval in seconds.
--- @field maxBufferPoints number Max offline buffer capacity.
--- @field maxBufferBytes number Max offline buffer size in bytes.
--- @field outageThreshold number Outage notification threshold in seconds.

--- @class InfluxClient
--- @field _config InfluxConfig
--- @field _connected boolean
local InfluxClient = {}
InfluxClient.__index = InfluxClient

--- Create a new InfluxClient instance.
--- @return InfluxClient
function InfluxClient:new()
  log:trace("InfluxClient:new()")
  local instance = setmetatable({}, self)
  instance._config = {
    url = "",
    token = "",
    database = "",
    precision = constants.DEFAULT_PRECISION,
    writeInterval = constants.DEFAULT_WRITE_INTERVAL,
    maxBufferPoints = constants.MAX_BUFFER_SIZE,
    maxBufferBytes = constants.MAX_BUFFER_BYTES,
    outageThreshold = constants.DEFAULT_OUTAGE_THRESHOLD,
  }
  instance._connected = false
  return instance
end

---------------------------------------------------------------------------
-- Configuration
---------------------------------------------------------------------------

--- Merge configuration values.
--- @param opts table Partial config to merge (only provided keys are updated).
function InfluxClient:configure(opts)
  log:trace("InfluxClient:configure(opts)")
  if not opts then
    return
  end
  for k, v in pairs(opts) do
    self._config[k] = v
  end
end

--- Return the current config table.
--- @return InfluxConfig
function InfluxClient:getConfig()
  log:trace("InfluxClient:getConfig()")
  return self._config
end

--- Check if connection config is complete (URL + database set).
--- @return boolean
function InfluxClient:isConfigured()
  log:trace("InfluxClient:isConfigured()")
  return self._config.url ~= "" and self._config.database ~= ""
end

--- Auto-test connection when config changes. Only fires when both URL and
--- database are configured. Call this from OPC handlers for connection properties.
function InfluxClient:checkConnection()
  log:trace("InfluxClient:checkConnection()")
  if self:isConfigured() then
    self:testConnection()
  else
    self:updateConnectionStatus(false, "Not configured")
  end
end

--- Return whether currently connected.
--- @return boolean
function InfluxClient:isConnected()
  log:trace("InfluxClient:isConnected()")
  return self._connected
end

---------------------------------------------------------------------------
-- URL & Header Helpers
---------------------------------------------------------------------------

--- Build the full InfluxDB write API endpoint URL.
--- @return string|nil url The write endpoint URL, or nil if config is incomplete.
function InfluxClient:getWriteUrl()
  log:trace("InfluxClient:getWriteUrl()")
  if self._config.url == "" or self._config.database == "" then
    return nil
  end
  local base = self._config.url:gsub("/$", "")
  return string.format("%s/api/v2/write?bucket=%s&precision=%s", base, self._config.database, self._config.precision)
end

--- Build HTTP headers for InfluxDB requests.
--- @return table<string, string>
function InfluxClient:getHeaders()
  log:trace("InfluxClient:getHeaders()")
  local headers = {
    ["Content-Type"] = "text/plain; charset=utf-8",
  }
  if self._config.token ~= "" then
    headers["Authorization"] = "Token " .. self._config.token
  end
  return headers
end

---------------------------------------------------------------------------
-- Connection Status
---------------------------------------------------------------------------

--- Update the connection status display and fire events/conditionals.
--- @param connected boolean
--- @param status string|nil Optional status text override.
function InfluxClient:updateConnectionStatus(connected, status)
  log:trace("InfluxClient:updateConnectionStatus(%s, '%s')", tostring(connected), tostring(status))
  local changed = self._connected ~= connected
  self._connected = connected

  local statusText = status or (connected and "Connected" or "Disconnected")
  UpdateProperty("Driver Status", statusText)

  if changed then
    -- Update the XML-defined conditional test function
    TC.INFLUXDB_CONNECTED = function()
      return connected
    end
    if connected then
      C4:FireEvent("Connected")
    else
      C4:FireEvent("Disconnected")
    end
  end
end

--- Test connectivity to InfluxDB by probing the write endpoint.
--- Called automatically when connection settings change and manually via
--- the TestConnection action.
function InfluxClient:testConnection()
  log:trace("InfluxClient:testConnection()")
  if self._config.url == "" then
    self:updateConnectionStatus(false, "No URL configured")
    log:warn("Test Connection: No InfluxDB URL configured")
    return
  end

  local base = self._config.url:gsub("/$", "")
  local pingUrl = base .. "/ping"

  log:info("Testing connection to %s", pingUrl)
  self:updateConnectionStatus(false, "Testing...")

  -- Test connectivity by writing an empty body to the write API.
  -- A successful auth returns 200/204 (no data) or 422 (empty body parse error),
  -- while auth failures return 401/403.
  local writeUrl = self:getWriteUrl()
  if not writeUrl then
    self:updateConnectionStatus(false, "Incomplete config")
    log:warn("Test Connection: database not configured")
    return
  end

  http:post(writeUrl, "", self:getHeaders()):next(function(response)
    log:info("Connection test successful (HTTP %d)", response.code)
    self:updateConnectionStatus(true, "Connected")
  end, function(err)
    -- 400/422 means the server received our request and auth passed — just no valid data
    if err.code == 400 or err.code == 422 or err.code == 204 then
      log:info("Connection test successful (auth OK, HTTP %d)", err.code)
      self:updateConnectionStatus(true, "Connected")
    else
      local statusMsg = err.code and string.format("HTTP %d", err.code) or ("Error: " .. (err.error or "unknown"))
      log:error("Connection test failed: %s", err.error or "unknown")
      self:updateConnectionStatus(false, statusMsg)
    end
  end)
end

return InfluxClient
