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
local constants = require("constants")
local OfflineBuffer = require("lib.offline_buffer")
local InfluxWriter = require("lib.influx_writer")
local SubscriptionEngine = require("lib.subscriptions")
local MeasurementManager = require("lib.measurements")
local InfluxClient = require("lib.influx_client")

---------------------------------------------------------------------------
-- State
---------------------------------------------------------------------------

--- Driver initialization flag.
--- @type boolean
local gInitialized = false

--#ifndef DRIVERCENTRAL
--- Whether this instance is the leader (lowest device ID) for update checks.
--- @type boolean
local isLeaderInstance = false
--#endif

--- InfluxDB connection client.
--- @type InfluxClient
local influxClient = InfluxClient:new()

--- Whether a drain cycle is currently in flight (waiting for HTTP callback).
--- @type boolean
local drainInFlight = false

--- Offline buffer instance for retry and persistence (initialized in OnDriverLateInit).
--- @type OfflineBuffer?
local offlineBuffer

--- Measurement manager instance (initialized in OnDriverLateInit).
--- @type MeasurementManager?
local measManager

--- InfluxWriter batch engine instance (initialized in OnDriverLateInit).
--- @type InfluxWriter?
local influxWriter

--- Subscription engine instance (initialized in OnDriverLateInit).
--- @type SubscriptionEngine?
local subEngine

--#ifndef DRIVERCENTRAL
--- Get all device IDs for instances of this driver, sorted ascending.
--- @return integer[]
local function getDriverIds()
  local drivers = C4:GetDevicesByC4iName(C4:GetDriverFileName()) or {}
  local ids = {}
  for id, _ in pairs(drivers) do
    table.insert(ids, tointeger(id))
  end
  table.sort(ids)
  return ids
end

--- Sync a property value to all other instances of this driver.
--- Only syncs if the other instance has a different value (avoids infinite loops).
--- @param propertyName string
--- @param propertyValue string
local function syncPropertyToOtherInstances(propertyName, propertyValue)
  local ids = getDriverIds()
  local myId = C4:GetDeviceID()
  for _, deviceId in ipairs(ids) do
    if deviceId ~= myId then
      local props = GetDeviceProperties(deviceId)
      if Select(props, propertyName) ~= propertyValue then
        C4:SendUIRequest(
          C4:GetProxyDevices(deviceId) or deviceId,
          "PROPERTY",
          { Name = propertyName, Value = propertyValue }
        )
      end
    end
  end
end
--#endif

---------------------------------------------------------------------------
-- Helpers
---------------------------------------------------------------------------

--- Handle an offline-buffer state change (called from OfflineBuffer callbacks).
--- @param state string One of "Connected", "Disconnected", "Reconnecting"
local function onBufferStateChange(state)
  UpdateProperty("Connection State", state)

  if state == OfflineBuffer.State.CONNECTED then
    influxClient:updateConnectionStatus(true)
    if offlineBuffer then
      UpdateProperty("Offline Buffer Size", tostring(offlineBuffer:size()))
    end
  elseif state == OfflineBuffer.State.RECONNECTING then
    influxClient:updateConnectionStatus(false, "Reconnecting...")
  else
    influxClient:updateConnectionStatus(false, "Disconnected")
  end
end

--- Handle extended-outage notification from the offline buffer.
local function onOutageThreshold()
  log:warn("Extended InfluxDB outage — firing Extended Outage event")
  C4:FireEvent("Extended Outage")
end

--- Drain callback registered with the offline buffer.
--- Called when the buffer decides it's time to attempt delivery.
--- Uses the Deferred-based InfluxWriter.postBatch() for consistent async patterns.
--- @param points string[] Points to send (the full buffer contents).
local function drainOfflineBuffer(points)
  if drainInFlight then
    log:debug("Drain already in flight, skipping")
    return
  end

  local url = influxClient:getWriteUrl()
  if not url then
    return
  end

  local cfg = influxClient:getConfig()
  local count = math.min(#points, constants.MAX_BATCH_SIZE)
  local batch = {}
  for i = 1, count do
    batch[i] = points[i]
  end

  drainInFlight = true
  InfluxWriter.postBatch(url, cfg.token, batch):next(function()
    drainInFlight = false
    offlineBuffer:onDrainResult(true, count, false)
  end, function(err)
    drainInFlight = false
    offlineBuffer:onDrainResult(false, 0, err.retriable)
  end)
end

--- Initialize the InfluxWriter batch engine.
local function initInfluxWriter()
  influxWriter = InfluxWriter:new({
    getConfig = function()
      local cfg = influxClient:getConfig()
      return {
        url = cfg.url,
        token = cfg.token,
        database = cfg.database,
        precision = cfg.precision,
      }
    end,
    onConnected = function(connected)
      influxClient:updateConnectionStatus(connected)
    end,
    onWriteError = function(errMsg)
      log:error("InfluxWriter error: %s", errMsg or "unknown")
      C4:FireEvent("Write Error")
    end,
    onBufferFull = function()
      C4:FireEvent("Buffer Full")
    end,
  })
  log:info("InfluxWriter batch engine initialized")
end

---------------------------------------------------------------------------
-- Property Changed Handlers
---------------------------------------------------------------------------

--- @param propertyValue string
function OPC.Automatic_Updates(propertyValue)
  log:trace("OPC.Automatic_Updates('%s')", propertyValue)
  --#ifndef DRIVERCENTRAL
  if not gInitialized and not isLeaderInstance then
    return
  end
  syncPropertyToOtherInstances("Automatic Updates", propertyValue)
  --#endif
end

--#ifndef DRIVERCENTRAL
--- @param propertyValue string
function OPC.Update_Channel(propertyValue)
  log:trace("OPC.Update_Channel('%s')", propertyValue)
  if not gInitialized and not isLeaderInstance then
    return
  end
  syncPropertyToOtherInstances("Update Channel", propertyValue)
end
--#endif

--- @param propertyValue string
function OPC.Log_Level(propertyValue)
  log:trace("OPC.Log_Level('%s')", propertyValue)
  log:setLogLevel(propertyValue)
end

--- @param propertyValue string
function OPC.Max_Buffer_Size(propertyValue)
  log:trace("OPC.Max_Buffer_Size('%s')", propertyValue)
  local n = tonumber(propertyValue)
  if n and n > 0 then
    influxClient:configure({ maxBufferPoints = n })
    if offlineBuffer then
      offlineBuffer:configure({ max_points = n })
    end
  end
end

--- @param propertyValue string
function OPC.Outage_Notification_Threshold(propertyValue)
  log:trace("OPC.Outage_Notification_Threshold('%s')", propertyValue)
  local secs = constants.OUTAGE_THRESHOLDS[propertyValue]
  if not secs then
    secs = tonumber(propertyValue)
  end
  if secs and secs > 0 then
    influxClient:configure({ outageThreshold = secs })
    if offlineBuffer then
      offlineBuffer:configure({ outage_threshold = secs })
    end
  end
end

--- @param propertyValue string
function OPC.Log_Mode(propertyValue)
  log:trace("OPC.Log_Mode('%s')", propertyValue)
  log:setLogMode(propertyValue)
end

--- @param propertyValue string
function OPC.InfluxDB_URL(propertyValue)
  log:trace("OPC.InfluxDB_URL('%s')", propertyValue)
  influxClient:configure({ url = propertyValue or "" })
  if gInitialized then
    influxClient:checkConnection()
  end
end

--- @param propertyValue string
function OPC.API_Token(propertyValue)
  log:trace("OPC.API_Token(<redacted>)")
  influxClient:configure({ token = propertyValue or "" })
  if gInitialized then
    influxClient:checkConnection()
  end
end

--- @param propertyValue string
function OPC.Database(propertyValue)
  log:trace("OPC.Database('%s')", propertyValue)
  influxClient:configure({ database = propertyValue or "" })
  if gInitialized then
    influxClient:checkConnection()
  end
end

--- @param propertyValue string
function OPC.Write_Precision(propertyValue)
  log:trace("OPC.Write_Precision('%s')", propertyValue)
  influxClient:configure({ precision = propertyValue or constants.DEFAULT_PRECISION })
end

--- @param propertyValue string
function OPC.Default_Write_Interval(propertyValue)
  log:trace("OPC.Default_Write_Interval('%s')", propertyValue)
  local seconds = constants.WRITE_INTERVALS[propertyValue]
  if seconds then
    influxClient:configure({ writeInterval = seconds })
  end
end

--- Handle the "Select Measurement" property change.
--- Updates visibility of configuration properties based on the selected measurement.
--- @param propertyValue string
function OPC.Configure_Measurement(propertyValue)
  log:trace("OPC.Configure_Measurement('%s')", propertyValue)
  if not gInitialized then
    return
  end
  measManager:setSelected(propertyValue)
end

--- Handle the "Measurement Write Interval" property change for the selected measurement.
--- @param propertyValue string
function OPC.Measurement_Write_Interval(propertyValue)
  log:trace("OPC.Measurement_Write_Interval('%s')", propertyValue)
  if not gInitialized then
    return
  end
  measManager:setInterval(propertyValue)
end

--- Handle the "Measurement Enabled" property change for the selected measurement.
--- @param propertyValue string
function OPC.Measurement_Enabled(propertyValue)
  log:trace("OPC.Measurement_Enabled('%s')", propertyValue)
  if not gInitialized then
    return
  end
  measManager:setEnabled(propertyValue == "On", subEngine)
end

--- Handle the "Add Field" variable selector change.
--- @param propertyValue string
function OPC.Add_Field(propertyValue)
  log:trace("OPC.Add_Field('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if IsEmpty(propertyValue) then
    return
  end
  measManager:addVariable("fields", propertyValue, subEngine)
end

--- Handle the "Add Tag" variable selector change.
--- @param propertyValue string
function OPC.Add_Tag(propertyValue)
  log:trace("OPC.Add_Tag('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if IsEmpty(propertyValue) then
    return
  end
  measManager:addVariable("tags", propertyValue, subEngine)
end

--- Handle the "Remove Field" property change.
--- @param propertyValue string
function OPC.Remove_Field(propertyValue)
  log:trace("OPC.Remove_Field('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if propertyValue == constants.SELECT_OPTION or IsEmpty(propertyValue) then
    return
  end
  measManager:removeVariable("fields", propertyValue, subEngine)
end

--- Handle the "Remove Tag" property change.
--- @param propertyValue string
function OPC.Remove_Tag(propertyValue)
  log:trace("OPC.Remove_Tag('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if propertyValue == constants.SELECT_OPTION or IsEmpty(propertyValue) then
    return
  end
  measManager:removeVariable("tags", propertyValue, subEngine)
end

--- Handle the "Add Measurement" property change.
--- Creates a new measurement when a non-empty name is set, then clears the field.
--- @param propertyValue string
function OPC.Add_Measurement(propertyValue)
  log:trace("OPC.Add_Measurement('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if IsEmpty(propertyValue) then
    return
  end
  measManager:add(propertyValue)
  measManager:updateLists()
end

--- Handle the "Remove Measurement" property change.
--- Removes the selected measurement from configuration.
--- @param propertyValue string
function OPC.Remove_Measurement(propertyValue)
  log:trace("OPC.Remove_Measurement('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if propertyValue == constants.SELECT_OPTION or not propertyValue or propertyValue == "" then
    return
  end
  measManager:remove(propertyValue, subEngine, influxWriter)
  measManager:updateLists()
end

---------------------------------------------------------------------------
-- Action Handlers (via ExecuteCommand / EC table)
---------------------------------------------------------------------------

--#ifndef DRIVERCENTRAL
--- Update Drivers action handler.
function EC.Update_Drivers()
  log:trace("EC.Update_Drivers()")
  log:print("Updating drivers")
  UpdateDrivers(true)
end
--#endif

--- Clear offline buffer action handler.
function EC.ClearOfflineBuffer()
  log:info("Action: Clear Offline Buffer")
  if offlineBuffer then
    offlineBuffer:clear()
  end
end

--#ifndef DRIVERCENTRAL
--- Update the driver from the GitHub repository.
--- @param forceUpdate? boolean Force the update even if the driver is up to date.
function UpdateDrivers(forceUpdate)
  log:trace("UpdateDrivers(%s)", forceUpdate)
  githubUpdater
    :updateAll(DRIVER_GITHUB_REPO, DRIVER_FILENAMES, Properties["Update Channel"] == "Prerelease", forceUpdate)
    :next(function(updatedDrivers)
      if not IsEmpty(updatedDrivers) then
        log:info("Updated driver(s): %s", table.concat(updatedDrivers, ","))
      else
        log:info("No driver updates available")
      end
    end, function(error)
      log:error("An error occurred updating drivers: %s", error)
    end)
end
--#endif

---------------------------------------------------------------------------
-- Driver Lifecycle
---------------------------------------------------------------------------

function OnDriverInit()
  --#ifdef DRIVERCENTRAL
  require("cloud-client-byte")
  C4:AllowExecute(false)
  --#else
  C4:AllowExecute(true)
  --#endif
  gInitialized = false
  log:setLogName(C4:GetDeviceData(C4:GetDeviceID(), "name"))
  log:setLogLevel(Properties["Log Level"])
  log:setLogMode(Properties["Log Mode"])
  log:trace("OnDriverInit()")
end

function OnDriverLateInit()
  log:trace("OnDriverLateInit()")

  C4:FileSetDir("c29tZXNwZWNpYWxrZXk=++11")

  -- Set driver version
  UpdateProperty("Driver Version", C4:GetDeviceData(C4:GetDeviceID(), "version"))

  --#ifndef DRIVERCENTRAL
  isLeaderInstance = Select(getDriverIds(), 1) == C4:GetDeviceID()
  --#endif

  log:info("InfluxDB Data Logger initializing")

  -- Load config from properties
  local intervalStr = Properties["Default Write Interval"] or "1m"
  local maxBufStr = Properties["Max Buffer Size"]
  local outageStr = Properties["Outage Notification Threshold"] or "5m"
  local outageSecs = constants.OUTAGE_THRESHOLDS[outageStr]

  influxClient:configure({
    url = Properties["InfluxDB URL"] or "",
    token = Properties["API Token"] or "",
    database = Properties["Database"] or "",
    precision = Properties["Write Precision"] or constants.DEFAULT_PRECISION,
    writeInterval = constants.WRITE_INTERVALS[intervalStr] or constants.DEFAULT_WRITE_INTERVAL,
    maxBufferPoints = tonumber(maxBufStr) or constants.MAX_BUFFER_SIZE,
    outageThreshold = outageSecs or tonumber(outageStr) or constants.DEFAULT_OUTAGE_THRESHOLD,
  })

  -- Initialize offline buffer
  local cfg = influxClient:getConfig()
  offlineBuffer = OfflineBuffer:new({
    max_points = cfg.maxBufferPoints,
    max_bytes = cfg.maxBufferBytes,
    outage_threshold = cfg.outageThreshold,
  })
  offlineBuffer:setCallbacks(drainOfflineBuffer, onBufferStateChange, onOutageThreshold)

  local bufferedCount = offlineBuffer:size()
  if bufferedCount > 0 then
    log:info("Resuming: %d point(s) in offline buffer from previous session", bufferedCount)
  end

  -- Initialize measurement manager (loads from persist)
  measManager = MeasurementManager:new()
  measManager:updateLists()
  measManager:refreshUI()

  -- Initialize InfluxWriter batch engine
  initInfluxWriter()

  -- Initialize subscription engine
  subEngine = SubscriptionEngine:new({
    getMeasurements = function()
      return measManager:getAll()
    end,
    getInfluxWriter = function()
      return influxWriter
    end,
    getWriteInterval = function()
      return influxClient:getConfig().writeInterval
    end,
  })

  -- Re-subscribe to all variables from persisted measurement configs
  subEngine:resubscribeAll()

  -- Fire OnPropertyChanged for all properties to ensure consistent state
  for p, _ in pairs(Properties) do
    local status, err = pcall(OnPropertyChanged, p)
    if not status then
      log:error(tostring(err))
    end
  end

  gInitialized = true

  -- Auto-connect if configured
  influxClient:checkConnection()

  --#ifndef DRIVERCENTRAL
  -- Periodic update check (every 30 minutes, leader instance only)
  SetTimer("UpdateCheck", 30 * 60 * 1000, function()
    -- Recompute leader each cycle in case the previous leader was removed
    isLeaderInstance = Select(getDriverIds(), 1) == C4:GetDeviceID()
    if isLeaderInstance and toboolean(Properties["Automatic Updates"]) then
      log:info("Checking for driver update (leader instance)")
      UpdateDrivers()
    end
  end, true)
  --#endif

  log:info("InfluxDB Data Logger initialized")
end

function OnDriverDestroyed()
  log:info("InfluxDB Data Logger shutting down")

  -- Shut down the InfluxWriter batch engine (flushes all per-measurement buffers)
  if influxWriter then
    influxWriter:shutdown()
  end

  -- Destroy offline buffer (cancels retry timers)
  if offlineBuffer then
    offlineBuffer:destroy()
    offlineBuffer = nil
  end
end

--- Handle removal of a device from the Control4 project.
--- Cleans up any variable subscriptions and cache entries for the removed device.
--- @param deviceId number
function OnDeviceRemoved(deviceId)
  log:info("OnDeviceRemoved: device %d", deviceId)
  subEngine:handleDeviceRemoved(deviceId)
end
