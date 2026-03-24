--#ifdef DRIVERCENTRAL
DC_PID = 0 -- TODO: Assign DriverCentral product ID
DC_X = nil
DC_FILENAME = "influxdb.c4z"
--#else
DRIVER_GITHUB_REPO = "finitelabs/control4-influxdb"
DRIVER_FILENAMES = {
  "influxdb.c4z",
}
--#endif

require("lib.utils")
require("drivers-common-public.global.handlers")
require("drivers-common-public.global.lib")
require("drivers-common-public.global.timer")
require("drivers-common-public.global.url")

local log = require("lib.logging")
--#ifndef DRIVERCENTRAL
local githubUpdater = require("lib.github-updater")
--#endif
local persist = require("lib.persist")
local events = require("lib.events")
local conditionals = require("lib.conditionals")
local constants = require("constants")

---------------------------------------------------------------------------
-- State
---------------------------------------------------------------------------

--- Whether we have a valid connection to InfluxDB.
--- @type boolean
local influxConnected = false

--- Current InfluxDB connection settings (populated from properties).
--- @type table
local config = {
  url = "",
  token = "",
  database = "",
  precision = constants.DEFAULT_PRECISION,
  writeInterval = constants.DEFAULT_WRITE_INTERVAL,
}

--- Measurement configurations keyed by measurement name.
--- Each entry: { fields = {}, tags = {}, interval = number, enabled = boolean }
--- @type table<string, table>
local measurements = {}

--- Write buffer: array of line-protocol strings waiting to be flushed.
--- @type string[]
local writeBuffer = {}

--- Flush timer ID.
--- @type number|nil
local flushTimerId = nil

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

--- Update the connection status display and fire events/conditionals.
--- @param connected boolean
--- @param status string|nil Optional status text override.
local function updateConnectionStatus(connected, status)
  local changed = influxConnected ~= connected
  influxConnected = connected

  local statusText = status or (connected and "Connected" or "Disconnected")
  UpdateProperty("Driver Status", statusText)

  if changed then
    conditionals.set("INFLUXDB_CONNECTED", connected)
    if connected then
      events.fire("Connected")
    else
      events.fire("Disconnected")
    end
  end
end

--- Build the base URL for InfluxDB write API.
--- @return string|nil url The full write endpoint URL, or nil if config is incomplete.
local function getWriteUrl()
  if config.url == "" or config.database == "" then
    return nil
  end
  -- Strip trailing slash from URL
  local base = config.url:gsub("/$", "")
  return string.format("%s/api/v2/write?db=%s&precision=%s", base, config.database, config.precision)
end

--- Build HTTP headers for InfluxDB requests.
--- @return table headers
local function getHeaders()
  local headers = {
    ["Content-Type"] = "text/plain; charset=utf-8",
  }
  if config.token ~= "" then
    headers["Authorization"] = "Token " .. config.token
  end
  return headers
end

--- Test connectivity to InfluxDB by hitting the /health endpoint.
local function testConnection()
  if config.url == "" then
    updateConnectionStatus(false, "No URL configured")
    log:warning("Test Connection: No InfluxDB URL configured")
    return
  end

  local base = config.url:gsub("/$", "")
  local healthUrl = base .. "/health"

  log:info("Testing connection to %s", healthUrl)
  updateConnectionStatus(false, "Testing...")

  C4:urlGet(healthUrl, getHeaders(), false, function(ticketId, strData, responseCode, tHeaders, strError)
    if strError and strError ~= "" then
      log:error("Connection test failed: %s", strError)
      updateConnectionStatus(false, "Error: " .. strError)
      return
    end

    if responseCode == 200 then
      log:info("Connection test successful (HTTP %d)", responseCode)
      updateConnectionStatus(true, "Connected")
    else
      log:error("Connection test failed (HTTP %d): %s", responseCode, strData or "")
      updateConnectionStatus(false, string.format("HTTP %d", responseCode))
    end
  end)
end

--- Load measurement configurations from persistent storage.
local function loadMeasurements()
  local data = persist.get("measurements")
  if data and type(data) == "table" then
    measurements = data
    log:info("Loaded %d measurement(s) from storage", Select(TableCount, measurements) or 0)
  else
    measurements = {}
  end
end

--- Save measurement configurations to persistent storage.
local function saveMeasurements()
  persist.set("measurements", measurements)
end

--- Add a new measurement configuration.
--- @param name string The measurement name.
local function addMeasurement(name)
  if not name or name == "" then
    log:warning("Cannot add measurement: name is empty")
    return
  end

  -- Sanitize: InfluxDB measurement names should not contain spaces or commas
  name = name:gsub("[%s,]", "_")

  if measurements[name] then
    log:warning("Measurement '%s' already exists", name)
    return
  end

  measurements[name] = {
    fields = {},
    tags = {},
    interval = config.writeInterval,
    enabled = true,
  }

  saveMeasurements()
  log:info("Added measurement: %s", name)

  -- TODO (DRV-8): Create dynamic properties for this measurement
  -- TODO (DRV-9): Set up variable subscriptions for this measurement
end

--- Remove a measurement configuration.
--- @param name string The measurement name.
local function removeMeasurement(name)
  if not measurements[name] then
    log:warning("Measurement '%s' does not exist", name)
    return
  end

  -- TODO (DRV-9): Unsubscribe from variables for this measurement
  -- TODO (DRV-8): Remove dynamic properties for this measurement

  measurements[name] = nil
  saveMeasurements()
  log:info("Removed measurement: %s", name)
end

--- Flush the write buffer to InfluxDB.
local function flushBuffer()
  if #writeBuffer == 0 then
    log:debug("Flush: buffer is empty, nothing to write")
    return
  end

  local url = getWriteUrl()
  if not url then
    log:warning("Flush: cannot write, InfluxDB not fully configured")
    return
  end

  -- Take a snapshot of the buffer and clear it
  local batch = {}
  local count = math.min(#writeBuffer, constants.MAX_BATCH_SIZE)
  for i = 1, count do
    batch[i] = writeBuffer[i]
  end

  -- Remove flushed entries from the buffer
  local remaining = {}
  for i = count + 1, #writeBuffer do
    remaining[#remaining + 1] = writeBuffer[i]
  end
  writeBuffer = remaining

  local payload = table.concat(batch, "\n")
  log:info("Flushing %d point(s) to InfluxDB (%d remaining in buffer)", count, #writeBuffer)

  C4:urlPost(url, payload, getHeaders(), false, function(ticketId, strData, responseCode, tHeaders, strError)
    if strError and strError ~= "" then
      log:error("Write failed: %s", strError)
      updateConnectionStatus(false, "Write error: " .. strError)
      events.fire("Write Error")
      -- TODO (DRV-12): Re-queue failed batch with backoff
      return
    end

    if responseCode == 200 or responseCode == 204 then
      log:debug("Write successful (HTTP %d), %d points written", responseCode, count)
      if not influxConnected then
        updateConnectionStatus(true)
      end
    elseif responseCode == 401 then
      log:error("Write failed: authentication error (HTTP 401). Check API token.")
      updateConnectionStatus(false, "Auth error (401)")
      events.fire("Write Error")
    elseif responseCode == 422 then
      log:error("Write failed: parse error (HTTP 422). Bad line protocol: %s", strData or "")
      events.fire("Write Error")
      -- Don't re-queue parse errors (permanent failure)
    elseif responseCode == 429 then
      log:warning("Write throttled (HTTP 429). Will retry.")
      events.fire("Write Error")
      -- TODO (DRV-12): Respect Retry-After header and re-queue
    else
      log:error("Write failed (HTTP %d): %s", responseCode, strData or "")
      updateConnectionStatus(false, string.format("Write error (HTTP %d)", responseCode))
      events.fire("Write Error")
      -- TODO (DRV-12): Re-queue with backoff for 5xx errors
    end
  end)
end

---------------------------------------------------------------------------
-- Property Changed Handlers
---------------------------------------------------------------------------

--- @param propertyValue string
function OPC.Log_Level(propertyValue)
  log:setLevel(propertyValue)
end

--- @param propertyValue string
function OPC.Log_Mode(propertyValue)
  log:setMode(propertyValue)
end

--- @param propertyValue string
function OPC.InfluxDB_URL(propertyValue)
  log:trace("OPC.InfluxDB_URL('%s')", propertyValue)
  config.url = propertyValue or ""
end

--- @param propertyValue string
function OPC.API_Token(propertyValue)
  log:trace("OPC.API_Token(<redacted>)")
  config.token = propertyValue or ""
end

--- @param propertyValue string
function OPC.Database(propertyValue)
  log:trace("OPC.Database('%s')", propertyValue)
  config.database = propertyValue or ""
end

--- @param propertyValue string
function OPC.Write_Precision(propertyValue)
  log:trace("OPC.Write_Precision('%s')", propertyValue)
  config.precision = propertyValue or constants.DEFAULT_PRECISION
end

--- @param propertyValue string
function OPC.Default_Write_Interval(propertyValue)
  log:trace("OPC.Default_Write_Interval('%s')", propertyValue)
  local seconds = constants.WRITE_INTERVALS[propertyValue]
  if seconds then
    config.writeInterval = seconds
  end
end

---------------------------------------------------------------------------
-- Action Handlers (via ExecuteCommand / EC table)
---------------------------------------------------------------------------

--- Test Connection action handler.
function EC.TestConnection()
  log:info("Action: Test Connection")
  testConnection()
end

--- Add Measurement action handler.
function EC.AddMeasurement()
  local name = Properties["Measurement Name"]
  log:info("Action: Add Measurement '%s'", name or "")
  addMeasurement(name)
  -- Clear the input field after adding
  UpdateProperty("Measurement Name", "")
end

--- Flush Buffer action handler.
function EC.FlushBuffer()
  log:info("Action: Flush Buffer (%d points buffered)", #writeBuffer)
  flushBuffer()
end

---------------------------------------------------------------------------
-- Driver Lifecycle
---------------------------------------------------------------------------

function OnDriverLateInit()
  log:info("InfluxDB Data Logger initializing")

  -- Set driver version
  UpdateProperty("Driver Version", C4:GetDeviceData(C4:GetDeviceID(), "version"))

  -- Initialize logging from current property values
  log:setLevel(Properties["Log Level"])
  log:setMode(Properties["Log Mode"])

  -- Load config from properties
  config.url = Properties["InfluxDB URL"] or ""
  config.token = Properties["API Token"] or ""
  config.database = Properties["Database"] or ""
  config.precision = Properties["Write Precision"] or constants.DEFAULT_PRECISION
  local intervalStr = Properties["Default Write Interval"] or "1m"
  config.writeInterval = constants.WRITE_INTERVALS[intervalStr] or constants.DEFAULT_WRITE_INTERVAL

  -- Load saved measurement configurations
  loadMeasurements()

  -- Fire OnPropertyChanged for all properties to ensure consistent state
  for p, _ in pairs(Properties) do
    local status, err = pcall(OnPropertyChanged, p)
    if not status then
      log:error(tostring(err))
    end
  end

  -- Start with disconnected status until explicitly tested
  updateConnectionStatus(false, "Not tested")

  log:info("InfluxDB Data Logger initialized")
end

function OnDriverDestroyed()
  log:info("InfluxDB Data Logger shutting down")

  -- Cancel flush timer if active
  if flushTimerId then
    C4:KillTimer(flushTimerId)
    flushTimerId = nil
  end

  -- Attempt to flush remaining buffer
  if #writeBuffer > 0 then
    log:info("Flushing %d remaining points before shutdown", #writeBuffer)
    flushBuffer()
  end
end
