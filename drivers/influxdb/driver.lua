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
local OfflineBuffer = require("lib.offline_buffer")

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
  maxBufferPoints = constants.MAX_BUFFER_SIZE,
  maxBufferBytes = constants.MAX_BUFFER_BYTES,
  outageThreshold = constants.DEFAULT_OUTAGE_THRESHOLD,
}

--- Whether a drain cycle is currently in flight (waiting for HTTP callback).
--- Prevents overlapping drain attempts.
--- @type boolean
local drainInFlight = false

--- Offline buffer instance for retry and persistence.
--- @type OfflineBuffer
local offlineBuffer

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

--- Handle an offline-buffer state change (called from OfflineBuffer callbacks).
--- @param state string One of "Connected", "Disconnected", "Reconnecting"
local function onBufferStateChange(state)
  -- Update the read-only Connection State property
  UpdateProperty("Connection State", state)

  if state == OfflineBuffer.State.CONNECTED then
    updateConnectionStatus(true)
    -- Update offline buffer size display
    if offlineBuffer then
      UpdateProperty("Offline Buffer Size", tostring(offlineBuffer:size()))
    end
  elseif state == OfflineBuffer.State.RECONNECTING then
    updateConnectionStatus(false, "Reconnecting...")
  else
    updateConnectionStatus(false, "Disconnected")
  end
end

--- Handle extended-outage notification from the offline buffer.
local function onOutageThreshold()
  log:warn("Extended InfluxDB outage — firing Extended Outage event")
  events.fire("Extended Outage")
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
    log:warn("Test Connection: No InfluxDB URL configured")
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
    log:warn("Cannot add measurement: name is empty")
    return
  end

  -- Sanitize: InfluxDB measurement names should not contain spaces or commas
  name = name:gsub("[%s,]", "_")

  if measurements[name] then
    log:warn("Measurement '%s' already exists", name)
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
    log:warn("Measurement '%s' does not exist", name)
    return
  end

  -- TODO (DRV-9): Unsubscribe from variables for this measurement
  -- TODO (DRV-8): Remove dynamic properties for this measurement

  measurements[name] = nil
  saveMeasurements()
  log:info("Removed measurement: %s", name)
end

--- Determine whether an HTTP response code represents a retriable error.
--- Retriable: network errors, 429 (rate limit), 5xx (server errors).
--- Non-retriable (permanent): 401 (auth), 422 (bad line protocol), other 4xx.
--- @param responseCode number|nil HTTP status code (nil for network-level errors).
--- @return boolean isRetriable
local function isRetriableError(responseCode)
  if not responseCode then
    return true
  end -- network error
  if responseCode == 429 then
    return true
  end
  if responseCode >= 500 then
    return true
  end
  return false
end

--- Write a batch of line-protocol points to InfluxDB.
--- On failure, routes points to the offline buffer when the error is retriable.
--- @param batch string[] Line-protocol strings to write.
--- @param isDrain boolean True if this write is draining the offline buffer.
local function writeBatch(batch, isDrain)
  local url = getWriteUrl()
  if not url then
    log:warn("Write: cannot write, InfluxDB not fully configured")
    if not isDrain then
      -- Buffer points for later when config is complete
      offlineBuffer:onWriteFailure(true, batch)
    end
    return
  end

  local payload = table.concat(batch, "\n")
  log:info("Writing %d point(s) to InfluxDB%s", #batch, isDrain and " (drain)" or "")

  C4:urlPost(url, payload, getHeaders(), false, function(ticketId, strData, responseCode, tHeaders, strError)
    if strError and strError ~= "" then
      log:error("Write failed (network): %s", strError)
      events.fire("Write Error")
      if isDrain then
        drainInFlight = false
        offlineBuffer:onDrainResult(false, 0, true)
      else
        local evicted = offlineBuffer:onWriteFailure(true, batch)
        if evicted > 0 then
          events.fire("Buffer Full")
        end
      end
      return
    end

    if responseCode == 200 or responseCode == 204 then
      log:debug("Write successful (HTTP %d), %d point(s) written", responseCode, #batch)
      events.fire("Connected") -- fire only when going from offline to online handled by state machine
      if isDrain then
        drainInFlight = false
        offlineBuffer:onDrainResult(true, #batch, false)
      else
        if not influxConnected then
          updateConnectionStatus(true)
        end
      end
    elseif responseCode == 401 then
      log:error("Write failed: authentication error (HTTP 401) — check API token")
      updateConnectionStatus(false, "Auth error (401)")
      events.fire("Write Error")
      if isDrain then
        drainInFlight = false
        offlineBuffer:onDrainResult(false, 0, false) -- permanent — stop draining
      end
      -- Don't buffer: permanent error
    elseif responseCode == 422 then
      log:error("Write failed: line protocol parse error (HTTP 422): %s", strData or "")
      events.fire("Write Error")
      if isDrain then
        drainInFlight = false
        -- Drop the bad points from the buffer and continue draining
        offlineBuffer:onWriteSuccess(#batch)
      end
      -- Don't buffer: permanent error
    elseif responseCode == 429 then
      log:warn("Write throttled (HTTP 429) — buffering and retrying with backoff")
      events.fire("Write Error")
      if isDrain then
        drainInFlight = false
        offlineBuffer:onDrainResult(false, 0, true)
      else
        local evicted = offlineBuffer:onWriteFailure(true, batch)
        if evicted > 0 then
          events.fire("Buffer Full")
        end
      end
    else
      log:error("Write failed (HTTP %d): %s", responseCode, strData or "")
      updateConnectionStatus(false, string.format("Write error (HTTP %d)", responseCode))
      events.fire("Write Error")
      local retriable = isRetriableError(responseCode)
      if isDrain then
        drainInFlight = false
        offlineBuffer:onDrainResult(false, 0, retriable)
      else
        if retriable then
          local evicted = offlineBuffer:onWriteFailure(true, batch)
          if evicted > 0 then
            events.fire("Buffer Full")
          end
        end
      end
    end
  end)
end

--- Flush the in-memory write buffer to InfluxDB.
--- If offline, queues points directly into the offline buffer.
local function flushBuffer()
  if #writeBuffer == 0 then
    log:debug("Flush: buffer is empty, nothing to write")
    return
  end

  -- Take a snapshot of the buffer and clear it
  local batch = {}
  local count = math.min(#writeBuffer, constants.MAX_BATCH_SIZE)
  for i = 1, count do
    batch[i] = writeBuffer[i]
  end

  local remaining = {}
  for i = count + 1, #writeBuffer do
    remaining[#remaining + 1] = writeBuffer[i]
  end
  writeBuffer = remaining

  -- If we know we're offline, skip the network and go straight to the buffer
  if offlineBuffer:getState() ~= OfflineBuffer.State.CONNECTED and offlineBuffer:size() > 0 then
    log:debug("Flush: offline, routing %d point(s) directly to offline buffer", #batch)
    local evicted = offlineBuffer:push(batch) or 0
    if evicted > 0 then
      events.fire("Buffer Full")
    end
    return
  end

  writeBatch(batch, false)
end

--- Drain callback registered with the offline buffer.
--- Called when the buffer decides it's time to attempt delivery.
--- @param points string[] Points to send (the full buffer contents).
local function drainOfflineBuffer(points)
  if drainInFlight then
    log:debug("Drain already in flight, skipping")
    return
  end

  local count = math.min(#points, constants.MAX_BATCH_SIZE)
  local batch = {}
  for i = 1, count do
    batch[i] = points[i]
  end

  drainInFlight = true
  writeBatch(batch, true)
end

---------------------------------------------------------------------------
-- Property Changed Handlers
---------------------------------------------------------------------------

--- @param propertyValue string
function OPC.Log_Level(propertyValue)
  log:setLevel(propertyValue)
end

--- @param propertyValue string
function OPC.Max_Buffer_Size(propertyValue)
  log:trace("OPC.Max_Buffer_Size('%s')", propertyValue)
  local n = tonumber(propertyValue)
  if n and n > 0 then
    config.maxBufferPoints = n
    if offlineBuffer then
      offlineBuffer:configure({ max_points = n })
    end
  end
end

--- @param propertyValue string
function OPC.Outage_Notification_Threshold(propertyValue)
  log:trace("OPC.Outage_Notification_Threshold('%s')", propertyValue)
  local secs = constants.OUTAGE_THRESHOLDS and constants.OUTAGE_THRESHOLDS[propertyValue]
  if not secs then
    secs = tonumber(propertyValue)
  end
  if secs and secs > 0 then
    config.outageThreshold = secs
    if offlineBuffer then
      offlineBuffer:configure({ outage_threshold = secs })
    end
  end
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
  log:info("Action: Flush Buffer (%d points in-memory)", #writeBuffer)
  flushBuffer()
end

--- Force drain offline buffer action handler.
function EC.DrainOfflineBuffer()
  log:info("Action: Drain Offline Buffer (%d points buffered)", offlineBuffer and offlineBuffer:size() or 0)
  if offlineBuffer then
    offlineBuffer:triggerDrain()
  end
end

--- Clear offline buffer action handler.
function EC.ClearOfflineBuffer()
  log:info("Action: Clear Offline Buffer")
  if offlineBuffer then
    offlineBuffer:clear()
  end
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

  local maxBufStr = Properties["Max Buffer Size"]
  config.maxBufferPoints = tonumber(maxBufStr) or constants.MAX_BUFFER_SIZE

  local outageStr = Properties["Outage Notification Threshold"]
  local outageSecs = constants.OUTAGE_THRESHOLDS and constants.OUTAGE_THRESHOLDS[outageStr]
  config.outageThreshold = outageSecs or tonumber(outageStr) or constants.DEFAULT_OUTAGE_THRESHOLD

  -- Initialize offline buffer
  offlineBuffer = OfflineBuffer:new({
    max_points = config.maxBufferPoints,
    max_bytes = config.maxBufferBytes,
    outage_threshold = config.outageThreshold,
  })
  offlineBuffer:setCallbacks(drainOfflineBuffer, onBufferStateChange, onOutageThreshold)

  local bufferedCount = offlineBuffer:size()
  if bufferedCount > 0 then
    log:info("Resuming: %d point(s) in offline buffer from previous session", bufferedCount)
  end

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

  -- Attempt to flush remaining in-memory buffer
  if #writeBuffer > 0 then
    log:info("Flushing %d remaining in-memory point(s) before shutdown", #writeBuffer)
    flushBuffer()
  end

  -- Destroy offline buffer (cancels retry timers)
  if offlineBuffer then
    offlineBuffer:destroy()
    offlineBuffer = nil
  end
end
