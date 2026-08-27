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
local transform = require("lib.transform")
local agents = require("lib.agents")

---------------------------------------------------------------------------
-- State
---------------------------------------------------------------------------

--- Driver initialization flag.
--- @type boolean
local gInitialized = false

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
      log:info("Syncing property '%s' = '%s' to device %d", propertyName, propertyValue, deviceId)
      SetDeviceProperties(deviceId, { [propertyName] = propertyValue }, true)
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
  if not gInitialized then
    return
  end
  syncPropertyToOtherInstances("Automatic Updates", propertyValue)
  --#endif
end

--#ifndef DRIVERCENTRAL
--- @param propertyValue string
function OPC.Update_Channel(propertyValue)
  log:trace("OPC.Update_Channel('%s')", propertyValue)
  if not gInitialized then
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

---------------------------------------------------------------------------
-- Web UI Request Handlers (UIR table)
---------------------------------------------------------------------------

--- Send a response to the web UI via both return value (for REST) and
--- SendDataToUI (for socket push). Returns JSON for REST callers.
--- @param command string The response command name.
--- @param data table The response data.
--- @return string JSON response for REST callers.
local function uiRespond(command, data)
  C4:SendDataToUI(command, data)
  data._command = command
  return JSON:encode(data)
end

--- Send the full measurement configuration to the web UI.
function UIR._GET_CONFIG()
  log:trace("UIR.GET_CONFIG()")
  local config = measManager:getConfigData()
  return uiRespond("CONFIG_DATA", { config = JSON:encode(config) })
end

--- Send connection status and metrics to the web UI.
--- Reads from Properties (source of truth) for connection state.
function UIR._GET_STATUS()
  log:trace("UIR.GET_STATUS()")
  local metrics = influxWriter and influxWriter:getMetrics() or {}
  local status = {
    connectionState = influxClient:isConnected() and "Connected" or "Disconnected",
    url = Properties["InfluxDB URL"] or "",
    database = Properties["Database"] or "",
    pointsBuffered = metrics.pointsBuffered or 0,
    pointsWritten = metrics.pointsWritten or 0,
    pointsDropped = metrics.pointsDropped or 0,
    writeErrors = metrics.writeErrors or 0,
  }
  return uiRespond("STATUS_DATA", { status = JSON:encode(status) })
end

--- Whether a proxy merely restates the device that declares it. A proxy named
--- for itself -- a security panel's areas, a receiver's tuner -- must stay its
--- own entry or it vanishes from the picker.
--- @param proxy table|nil The proxy's device definition, if it is in the project.
--- @param owner table The declaring device's definition.
--- @return boolean
local function isRestatedProxy(proxy, owner)
  return proxy ~= nil and tostring(proxy.deviceName) == tostring(owner.deviceName)
end

--- Section heading for a member of a merged entry, taken from the proxy's .c4i.
--- @param dev table A device definition from C4:GetDevices().
--- @param isOwner boolean True for the driver itself rather than one of its proxies.
--- @return string label
local function proxySectionLabel(dev, isOwner)
  if isOwner then
    return "Device"
  end
  local file = tostring((dev or {}).driverFileName or "")
  local base = file:match("^(.*)%.c4i$") or file:match("^(.*)%.c4z$")
  return IsEmpty(base) and "Proxy" or base
end

--- Send the device list to the web UI with display names (Room > Device).
--- Uses C4:GetDevices() for efficiency (avoids parsing large XML from GetProjectItems).
---
--- Reports hasVariables rather than filtering: mapping rows can only bind a
--- variable, but Device Tags only needs the item to exist.
---
--- A driver and the proxies that restate it are folded into one entry via the
--- parent's `proxies` table. Entries that still share a label get the id.
function UIR._GET_DEVICES()
  log:trace("UIR.GET_DEVICES()")
  local devices = {}
  local allDevices = C4:GetDevices() or {}

  -- Proxy id -> owning driver id, for proxies that only restate their owner.
  local ownerOf = {}
  for id, dev in pairs(allDevices) do
    for proxyId in pairs(dev.proxies or {}) do
      local pid = tonumber(proxyId) or proxyId
      if isRestatedProxy(allDevices[pid], dev) then
        ownerOf[pid] = id
      end
    end
  end

  -- Collapse chains so a proxy of a proxy still lands in an entry rather than
  -- being dropped for belonging to a device that is not one itself.
  local rootOf, membersOf = {}, {}
  for id in pairs(allDevices) do
    local root, seen = id, { [id] = true }
    while ownerOf[root] ~= nil and not seen[ownerOf[root]] do
      root = ownerOf[root]
      seen[root] = true
    end
    rootOf[id] = root
    membersOf[root] = membersOf[root] or {}
    table.insert(membersOf[root], id)
  end

  for id, dev in pairs(allDevices) do
    if rootOf[id] == id then
      local memberIds = membersOf[id]
      table.sort(memberIds)

      local hasVariables = false
      for _, mid in ipairs(memberIds) do
        local ok, memberVars = pcall(C4.GetDeviceVariables, C4, mid)
        if ok and memberVars ~= nil and next(memberVars) ~= nil then
          hasVariables = true
          break
        end
      end

      local name = dev.deviceName or ("Device " .. id)
      local displayName = name
      if not IsEmpty(dev.roomName) then
        displayName = dev.roomName .. " > " .. name
      end
      devices[#devices + 1] = {
        id = id,
        name = name,
        displayName = displayName,
        roomName = dev.roomName or "",
        section = "Devices",
        hasVariables = hasVariables,
        memberIds = memberIds,
      }
    end
  end

  -- Absent from C4:GetDevices(), so added separately. "Agents" stands in as the
  -- room to keep the picker's "Room > Device" shape.
  -- Refreshed per load so a newly added agent appears without a restart.
  for agentId, agentName in pairs(agents.getAll(true)) do
    local ok, agentVars = pcall(C4.GetDeviceVariables, C4, agentId)
    devices[#devices + 1] = {
      id = agentId,
      name = agentName,
      displayName = "Agents > " .. agentName,
      roomName = "Agents",
      section = "Agents",
      hasVariables = ok and agentVars ~= nil and next(agentVars) ~= nil,
      memberIds = { agentId },
    }
  end
  local labelCounts = {}
  for _, d in ipairs(devices) do
    labelCounts[d.displayName] = (labelCounts[d.displayName] or 0) + 1
  end
  for _, d in ipairs(devices) do
    if labelCounts[d.displayName] > 1 then
      d.displayName = string.format("%s (%s)", d.displayName, d.id)
    end
  end
  -- Devices first so the section headings keep a fixed order.
  local sectionRank = { Devices = 1, Agents = 2 }
  table.sort(devices, function(a, b)
    local ra, rb = sectionRank[a.section] or 9, sectionRank[b.section] or 9
    if ra ~= rb then
      return ra < rb
    end
    return (a.displayName or "") < (b.displayName or "")
  end)
  return uiRespond("DEVICES_DATA", { devices = JSON:encode(devices) })
end

--- Send variables for a device entry to the web UI.
---
--- Returns the entry's variables and its folded proxies', each carrying the id
--- of the member that owns it -- which is what a mapping stores.
--- @param tParams table
function UIR._GET_DEVICE_VARIABLES(tParams)
  log:trace("UIR.GET_DEVICE_VARIABLES()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  local devId = tonumber(params.deviceId)
  if not devId then
    return
  end

  local entry = C4:GetDevices({ DeviceIds = tostring(devId) })[devId] or {}
  local members = { { id = devId, section = proxySectionLabel(entry, true), order = 1 } }
  local queue, seen = { { id = devId, dev = entry } }, { [devId] = true }
  while #queue > 0 do
    local cur = table.remove(queue, 1)
    -- pairs() over proxies has no defined order, so walk them by id: section
    -- order would otherwise vary between calls.
    local childIds = {}
    for proxyId in pairs((cur.dev or {}).proxies or {}) do
      childIds[#childIds + 1] = tonumber(proxyId) or proxyId
    end
    table.sort(childIds)
    for _, pid in ipairs(childIds) do
      local proxy = C4:GetDevices({ DeviceIds = tostring(pid) })[pid]
      if not seen[pid] and isRestatedProxy(proxy, cur.dev) then
        seen[pid] = true
        members[#members + 1] = { id = pid, section = proxySectionLabel(proxy, false), order = #members + 1 }
        queue[#queue + 1] = { id = pid, dev = proxy }
      end
    end
  end

  local vars = {}
  for _, member in ipairs(members) do
    local ok, deviceVars = pcall(C4.GetDeviceVariables, C4, member.id)
    if ok and deviceVars then
      for varId, varInfo in pairs(deviceVars) do
        vars[#vars + 1] = {
          id = tonumber(varId),
          deviceId = member.id,
          section = member.section,
          order = member.order,
          name = varInfo.name or ("var" .. varId),
          type = varInfo.type or "STRING",
          value = varInfo.value,
        }
      end
    end
  end
  -- Order on the member's position, not its label: comparing label strings ties
  -- when a proxy's .c4i is named Device, and the owner-first rule contradicts
  -- that tie. LuaJIT's table.sort does not reject the resulting cycle, it just
  -- silently stops putting the device's own variables first.
  table.sort(vars, function(a, b)
    if a.order ~= b.order then
      return a.order < b.order
    end
    return (a.name or "") < (b.name or "")
  end)
  return uiRespond("DEVICE_VARIABLES_DATA", {
    deviceId = tostring(devId),
    variables = JSON:encode(vars),
  })
end

--- Evaluate a transform expression on the driver side and return the result.
--- @param tParams table
function UIR._EVAL_TRANSFORM(tParams)
  log:trace("UIR._EVAL_TRANSFORM()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  local expression = params.expression or ""
  local rawValue = params.value
  local result, err
  if expression == "" then
    result = rawValue
  else
    result, err = transform.eval(expression, tostring(rawValue))
  end

  -- Fields preview with their type visible (0, 0.0, "0", false) so a lossy pin
  -- shows before it is saved. Serialisation is left out: the suffix is stripped
  -- below and tags preview unescaped.
  if not err and params.kind == "field" and result ~= nil then
    local valueType = params.valueType
    if IsEmpty(valueType) then
      valueType = InfluxWriter.inferValueType(result)
    end
    local formatted, ferr = InfluxWriter.formatFieldValue(result, valueType)
    if formatted then
      -- Display only; the write keeps the suffix.
      result = formatted:gsub("i$", "")
    else
      err = ferr
    end
  end

  -- Encoded, not sent as scalars: C4:SendDataToUI coerces a numeric string, so
  -- "50.0" would arrive as 50. The other handlers encode for the same reason.
  return uiRespond("TRANSFORM_RESULT", {
    payload = JSON:encode({
      id = params.id or "",
      result = result ~= nil and tostring(result) or "",
      error = err or "",
    }),
  })
end

--- Add a new measurement.
--- @param tParams table
function UIR._ADD_MEASUREMENT(tParams)
  log:trace("UIR.ADD_MEASUREMENT()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.name then
    measManager:add(params.name)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Delete a measurement.
--- @param tParams table
function UIR._DELETE_MEASUREMENT(tParams)
  log:trace("UIR.DELETE_MEASUREMENT()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.name then
    measManager:remove(params.name, subEngine, influxWriter)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Add a field definition to a measurement schema.
--- @param tParams table
function UIR._ADD_FIELD_DEF(tParams)
  log:trace("UIR.ADD_FIELD_DEF()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.name then
    measManager:addFieldDef(params.measurement, params.name)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Remove a field definition from a measurement schema.
--- @param tParams table
function UIR._REMOVE_FIELD_DEF(tParams)
  log:trace("UIR.REMOVE_FIELD_DEF()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.name then
    measManager:removeFieldDef(params.measurement, params.name, subEngine)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Pin or clear the InfluxDB type for a schema field.
--- @param tParams table
function UIR._SET_FIELD_TYPE(tParams)
  log:trace("UIR.SET_FIELD_TYPE()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.name then
    measManager:setFieldType(params.measurement, params.name, params.valueType)
  end
  return UIR._GET_CONFIG()
end

--- Add a tag definition to a measurement schema.
--- @param tParams table
function UIR._ADD_TAG_DEF(tParams)
  log:trace("UIR.ADD_TAG_DEF()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.name then
    measManager:addTagDef(params.measurement, params.name)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Remove a tag definition from a measurement schema.
--- @param tParams table
function UIR._REMOVE_TAG_DEF(tParams)
  log:trace("UIR.REMOVE_TAG_DEF()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.name then
    measManager:removeTagDef(params.measurement, params.name, subEngine)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Update measurement settings (interval, enabled).
--- @param tParams table
function UIR._UPDATE_MEAS_SETTINGS(tParams)
  log:trace("UIR.UPDATE_MEAS_SETTINGS()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement then
    measManager:updateSettings(params.measurement, {
      interval = params.interval,
      dedup = params.dedup,
      enabled = params.enabled,
    }, subEngine, influxWriter)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Add a reading to a measurement.
--- @param tParams table
function UIR._ADD_READING(tParams)
  log:trace("UIR.ADD_READING()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.label then
    measManager:addReading(params.measurement, params.label)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Remove a reading from a measurement.
--- @param tParams table
function UIR._REMOVE_READING(tParams)
  log:trace("UIR.REMOVE_READING()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.label then
    measManager:removeReading(params.measurement, params.label, subEngine, influxWriter)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Save a mapping for a reading.
--- @param tParams table
function UIR._SAVE_MAPPING(tParams)
  log:trace("UIR.SAVE_MAPPING()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.reading and params.name and params.mapping then
    measManager:setMapping(params.measurement, params.reading, params.name, params.mapping, subEngine)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Update reading enabled state.
--- @param tParams table
function UIR._UPDATE_READING_ENABLED(tParams)
  log:trace("UIR.UPDATE_READING_ENABLED()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if params.measurement and params.label then
    measManager:setReadingEnabled(params.measurement, params.label, params.enabled, subEngine)
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Include device_name and room_name tags for a device on a specific reading.
--- Adds tag defs if missing and creates literal mappings with transforms.
--- @param tParams table
function UIR._INCLUDE_DEVICE_TAGS(tParams)
  log:trace("UIR.INCLUDE_DEVICE_TAGS()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  if not params.measurement or not params.deviceId then
    return UIR._GET_CONFIG()
  end

  local measName = params.measurement
  local deviceId = tostring(params.deviceId)
  local readingLabel = params.reading

  -- Add tag defs if they don't exist
  measManager:addTagDef(measName, "device_name")
  measManager:addTagDef(measName, "room_name")

  local meas = measManager:get(measName)
  if meas then
    -- If a specific reading is provided, only set for that reading
    -- Otherwise set for all readings
    local readings = {}
    if readingLabel and meas.readings[readingLabel] then
      readings[readingLabel] = true
    else
      for label, _ in pairs(meas.readings) do
        readings[label] = true
      end
    end
    for label, _ in pairs(readings) do
      measManager:setMapping(measName, label, "device_name", {
        source = "literal",
        literal = deviceId,
        transform = "device_name(value)",
      }, subEngine)
      measManager:setMapping(measName, label, "room_name", {
        source = "literal",
        literal = deviceId,
        transform = "room_name(value)",
      }, subEngine)
    end
  end

  if subEngine then
    subEngine:restartIntervalTimers()
  end
  return UIR._GET_CONFIG()
end

--- Validate a transform expression.
--- @param tParams table
function UIR._VALIDATE_TRANSFORM(tParams)
  log:trace("UIR.VALIDATE_TRANSFORM()")
  local params = JSON:decode(C4:Base64Decode(tParams.DATA or "e30="))
  local valid, err = transform.validate(params.expression or "")
  C4:SendDataToUI("VALIDATE_RESULT", {
    valid = valid and "true" or "false",
    error = err or "",
  })
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

---------------------------------------------------------------------------
-- Auto Configure (one click measurement discovery)
---------------------------------------------------------------------------
-- Builds tv_usage / light_usage / security_status from LIVE device discovery.
-- Only the controller can resolve a variable NAME to the numeric variable id
-- that subscriptions require (varId = "deviceId:variableId"), so this must run
-- on the controller rather than from any offline tool.

-- Variable names confirmed against live Control4 OS 3.x systems.
local AUTO = {
  SECURITY    = { "PARTITION_STATE" },
  LIGHT_ON    = { "LIGHT_STATE" },
  LIGHT_LEVEL = { "BRIGHTNESS PERCENT", "BRIGHTNESS TARGET PERCENT", "PRESET_LEVEL" },
  TV_POWER    = { "POWER_STATE" },
  TV_INPUT    = { "CURRENT_INPUT" },
}
-- on/off robust to number (level>0), "On"/"Off", or "true"/"false".
local AUTO_T_ONOFF  = '(function() local n = tonumber(value); if n then return n > 0 and 1 or 0 end; local v = tostring(value):lower(); return (v == "on" or v == "true") and 1 or 0 end)()'
local AUTO_T_LEVEL  = 'tonumber(value) or 0'
local AUTO_T_SOURCE = 'tostring(value)'
-- Guaranteed numeric: DISARM* -> 0, *AWAY -> 2, any other ARM* -> 1, else 0.
-- (map() returns the raw value on no-match, so do not rely on "map(...) or 0".)
local AUTO_T_ARMED  =
  'tostring(value):upper():find("DISARM") and 0 or (tostring(value):upper():find("AWAY") and 2 or (tostring(value):upper():find("ARM") and 1 or 0))'

-- Service intelligence: fault catalog (Phase 1). One device_faults reading per
-- (device, variable) found. severity/text/alert flag are static per code and live
-- here / in the report engine, NOT in the time series. _BOOL variants preferred.
local FAULT_CATALOG = {
  { var = "OVER_TEMPERATURE",       subsystem = "lighting", code = "load_overtemp",  rule = "bool" },
  { var = "SHORT_CIRCUIT_DETECTED", subsystem = "lighting", code = "load_short",     rule = "bool" },
  { var = "OVER_RATED_WATTAGE",     subsystem = "lighting", code = "load_overwatt",  rule = "bool" },
  { var = "UPS_POWER_LOST_BOOL",    subsystem = "power",    code = "ups_on_battery", rule = "bool" },
  { var = "TROUBLE_TYPE",           subsystem = "security", code = "sec_trouble",    rule = "nonempty" },
  { var = "LAST_ARM_FAILED",        subsystem = "security", code = "sec_arm_failed", rule = "nonempty" },
}
local FAULT_T_BOOL     = '(function() local n = tonumber(value); if n then return n > 0 and 1 or 0 end; local v = tostring(value):lower(); return (v == "true" or v == "yes" or v == "on") and 1 or 0 end)()'
local FAULT_T_NONEMPTY = '(function() local v = tostring(value):lower(); return (v ~= "" and v ~= "none" and v ~= "0" and v ~= "false") and 1 or 0 end)()'

--- Build name(upper) -> numeric variable id for one device.
--- @param devId number
--- @return table<string, number>
local function autoVarIndex(devId)
  local index = {}
  local ok, vars = pcall(C4.GetDeviceVariables, C4, devId)
  if ok and type(vars) == "table" then
    for vid, info in pairs(vars) do
      if info and info.name then
        index[string.upper(info.name)] = tonumber(vid)
      end
    end
  end
  return index
end

--- First matching variable id from a list of candidate names.
local function autoFirst(index, names)
  for _, n in ipairs(names) do
    local vid = index[string.upper(n)]
    if vid then
      return vid
    end
  end
  return nil
end

local function autoCount(t)
  local n = 0
  for _ in pairs(t) do
    n = n + 1
  end
  return n
end

--- Discover devices and build the full measurements config by convention.
--- @return table config
local function buildAutoConfig()
  local site = Properties["Site"]
  if IsEmpty(site) then
    site = "home"
  end

  local lights, tvs, sec, faults = {}, {}, {}, {}
  local devices = C4:GetDevices() or {}
  for id, dev in pairs(devices) do
    local devId = tonumber(id)
    if devId then
      local name = dev.deviceName or ("Device " .. tostring(devId))
      local room = dev.roomName or ""
      local label = name .. " [" .. tostring(devId) .. "]"
      local idx = autoVarIndex(devId)
      local vnames = {}
      for vn in pairs(idx) do
        vnames[#vnames + 1] = vn
      end
      log:debug("AutoConfigure scan dev %d '%s' [%s] vars: %s", devId, name, room, table.concat(vnames, ", "))

      local secVid = autoFirst(idx, AUTO.SECURITY)
      local lightVid = autoFirst(idx, AUTO.LIGHT_ON)
      local powerVid = autoFirst(idx, AUTO.TV_POWER)
      local inputVid = autoFirst(idx, AUTO.TV_INPUT)

      if secVid then
        sec[label] = {
          enabled = true,
          mappings = {
            armed = { source = "variable", varId = devId .. ":" .. secVid, transform = AUTO_T_ARMED },
            panel_name = { source = "literal", literal = name },
            site = { source = "literal", literal = site },
          },
        }
        log:debug("AutoConfigure: security '%s' (dev %d)", name, devId)
      elseif lightVid then
        local mappings = {
          is_on = { source = "variable", varId = devId .. ":" .. lightVid, transform = AUTO_T_ONOFF },
          device_name = { source = "literal", literal = name },
          room_name = { source = "literal", literal = room },
          site = { source = "literal", literal = site },
        }
        local lvlVid = autoFirst(idx, AUTO.LIGHT_LEVEL)
        if lvlVid then
          mappings.level = { source = "variable", varId = devId .. ":" .. lvlVid, transform = AUTO_T_LEVEL }
        end
        lights[label] = { enabled = true, mappings = mappings }
        log:debug("AutoConfigure: light '%s' (dev %d)", name, devId)
      elseif powerVid and inputVid then
        tvs[label] = {
          enabled = true,
          mappings = {
            power_on = { source = "variable", varId = devId .. ":" .. powerVid, transform = AUTO_T_ONOFF },
            source_name = { source = "variable", varId = devId .. ":" .. inputVid, transform = AUTO_T_SOURCE },
            room_name = { source = "literal", literal = (not IsEmpty(room)) and room or name },
            display_name = { source = "literal", literal = name },
            site = { source = "literal", literal = site },
          },
        }
        log:debug("AutoConfigure: tv '%s' (dev %d)", name, devId)
      end
      -- Orthogonal fault pass: a device can be a usage device AND a fault source.
      for _, f in ipairs(FAULT_CATALOG) do
        local fvid = idx[string.upper(f.var)]
        if fvid then
          local xf = (f.rule == "nonempty") and FAULT_T_NONEMPTY or FAULT_T_BOOL
          faults[label .. "::" .. f.code] = {
            enabled = true,
            mappings = {
              fault_active = { source = "variable", varId = devId .. ":" .. fvid, transform = xf },
              fault_code = { source = "literal", literal = f.code },
              subsystem = { source = "literal", literal = f.subsystem },
              device_name = { source = "literal", literal = name },
              room_name = { source = "literal", literal = room },
              site = { source = "literal", literal = site },
            },
          }
          log:debug("AutoConfigure: fault '%s' on '%s' (dev %d)", f.code, name, devId)
        end
      end
    end
  end

  log:info(
    "AutoConfigure discovered: %d lights, %d rooms/tvs, %d security, %d faults (site=%s)",
    autoCount(lights),
    autoCount(tvs),
    autoCount(sec),
    autoCount(faults),
    site
  )

  return {
    light_usage = {
      fieldDefs = { "is_on", "level" },
      tagDefs = { "site", "device_name", "room_name" },
      interval = "5m",
      enabled = true,
      dedup = true,
      readings = lights,
    },
    tv_usage = {
      fieldDefs = { "power_on" },
      tagDefs = { "site", "room_name", "display_name", "source_name" },
      interval = "1m",
      enabled = true,
      dedup = true,
      readings = tvs,
    },
    security_status = {
      fieldDefs = { "armed" },
      tagDefs = { "site", "panel_name" },
      interval = "5m",
      enabled = true,
      dedup = false,
      readings = sec,
    },
    device_faults = {
      fieldDefs = { "fault_active" },
      tagDefs = { "site", "device_name", "room_name", "subsystem", "fault_code" },
      interval = "5m",
      enabled = true,
      dedup = false,
      readings = faults,
    },
  }
end

--- Auto Configure Measurements action. One click: discover devices live and
--- apply tv_usage / light_usage / security_status. Dedup is off so the interval
--- acts as a heartbeat (on time = sample count * interval).
function EC.Auto_Configure()
  log:info("Action: Auto Configure Measurements")
  local config = buildAutoConfig()
  local applied = 0
  for measName, measConfig in pairs(config) do
    measManager:applyMeasurementConfig(measName, measConfig, subEngine, influxWriter)
    applied = applied + 1
  end
  if subEngine then
    subEngine:restartIntervalTimers()
  end
  log:print("Auto Configure applied %d measurements", applied)
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

  -- Set driver version
  UpdateProperty("Driver Version", C4:GetDeviceData(C4:GetDeviceID(), "version"))

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
    local isLeaderInstance = Select(getDriverIds(), 1) == C4:GetDeviceID()
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
