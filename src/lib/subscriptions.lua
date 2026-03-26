--- Variable subscription engine for InfluxDB measurement data collection.
---
--- Manages registering/unregistering Control4 variable listeners, caching
--- variable values, and routing variable changes to the appropriate measurement
--- point enqueue calls via InfluxWriter.

local log = require("lib.logging")
local constants = require("constants")

---------------------------------------------------------------------------
-- Local Helpers
---------------------------------------------------------------------------

--- Parse a variable ID string "deviceId:variableId" into its numeric components.
--- @param varIdStr string e.g. "100:5"
--- @return number|nil deviceId
--- @return number|nil variableId
local function parseVarId(varIdStr)
  if type(varIdStr) ~= "string" then
    return nil, nil
  end
  local devStr, varStr = varIdStr:match("^(%d+):(%d+)$")
  if not devStr then
    return nil, nil
  end
  return tonumber(devStr), tonumber(varStr)
end

--- Look up the display name for a variable.
--- @param deviceId number
--- @param variableId number
--- @return string Human-readable variable name, or "var<id>" fallback.
local function getVariableName(deviceId, variableId)
  local ok, vars = pcall(C4.GetDeviceVariables, C4, deviceId)
  if ok and vars then
    local entry = vars[tostring(variableId)]
    if entry and entry.name and entry.name ~= "" then
      return entry.name
    end
  end
  return string.format("var%d", variableId)
end

--- Look up the display name for a device.
--- @param deviceId number
--- @return string Device display name, or "device<id>" fallback.
local function getDeviceName(deviceId)
  local ok, name = pcall(C4.GetDeviceDisplayName, C4, deviceId)
  if ok and name and name ~= "" then
    return name
  end
  return string.format("device%d", deviceId)
end

--- Coerce a raw variable value to an InfluxDB value type identifier.
--- @param val any
--- @return string valueType One of "integer", "float", "string", "boolean"
local function inferValueType(val)
  if val == nil then
    return constants.VALUE_TYPES.STRING
  end
  local s = tostring(val)
  local low = s:lower()
  if low == "true" or low == "false" then
    return constants.VALUE_TYPES.BOOLEAN
  end
  local n = tonumber(s)
  if n then
    if math.floor(n) == n and math.abs(n) < 2 ^ 53 then
      return constants.VALUE_TYPES.INTEGER
    end
    return constants.VALUE_TYPES.FLOAT
  end
  return constants.VALUE_TYPES.STRING
end

---------------------------------------------------------------------------
-- Module
---------------------------------------------------------------------------

--- @class VarCacheEntry
--- @field value string Current variable value.
--- @field timestamp number Last update timestamp (os.time).
--- @field varName string Human-readable variable name.

--- @class SubscriptionEngine
--- @field _varCache table<number, table<number, VarCacheEntry>>
--- @field _subscriptionRefs table<string, table<string, string>>
--- @field _subscribing boolean
--- @field _getMeasurements fun(): table<string, MeasurementConfig>
--- @field _getInfluxWriter fun(): InfluxWriter|nil
--- @field _getWriteInterval fun(): number
local SubscriptionEngine = {}
SubscriptionEngine.__index = SubscriptionEngine

--- Create a new SubscriptionEngine instance.
--- @param opts table Constructor options:
---   - getMeasurements: fun(): table<string, MeasurementConfig>
---   - getInfluxWriter: fun(): InfluxWriter|nil
---   - getWriteInterval: fun(): number
--- @return SubscriptionEngine
function SubscriptionEngine:new(opts)
  log:trace("SubscriptionEngine:new(opts)")
  opts = opts or {}
  local instance = setmetatable({}, self)
  instance._varCache = {}
  instance._subscriptionRefs = {}
  instance._subscribing = false
  instance._getMeasurements = opts.getMeasurements or function()
    return {}
  end
  instance._getInfluxWriter = opts.getInfluxWriter or function()
    return nil
  end
  instance._getWriteInterval = opts.getWriteInterval or function()
    return constants.DEFAULT_WRITE_INTERVAL
  end
  return instance
end

---------------------------------------------------------------------------
-- Private Methods
---------------------------------------------------------------------------

--- Build and enqueue an InfluxDB data point via influxWriter for a single measurement
--- when a field variable changes. Only enqueues if the measurement is enabled and all
--- configured field variables have at least one cached value.
--- @param measName string
function SubscriptionEngine:_enqueueMeasurementPoint(measName)
  log:trace("SubscriptionEngine:_enqueueMeasurementPoint('%s')", measName)
  local measurements = self._getMeasurements()
  local meas = measurements[measName]
  if not meas or meas.enabled == false then
    return
  end

  local influxWriter = self._getInfluxWriter()
  if not influxWriter then
    log:warn("enqueueMeasurementPoint: influxWriter not initialized")
    return
  end

  -- Build fields table for influxWriter
  local fields = {}
  for _, varIdStr in ipairs(meas.fields or {}) do
    local devId, varId = parseVarId(varIdStr)
    if devId and varId then
      local cached = self._varCache[devId] and self._varCache[devId][varId]
      if not cached then
        log:debug("enqueuePoint(%s): waiting for initial value of %s, skipping", measName, varIdStr)
        return
      end
      local varName = cached.varName or getVariableName(devId, varId)
      fields[varName] = {
        value = cached.value,
        type = inferValueType(cached.value),
      }
    end
  end

  if not next(fields) then
    log:debug("enqueuePoint(%s): no fields configured, skipping", measName)
    return
  end

  -- Build tags table for influxWriter
  local tags = {}
  for _, varIdStr in ipairs(meas.tags or {}) do
    local devId, varId = parseVarId(varIdStr)
    if devId and varId then
      local cached = self._varCache[devId] and self._varCache[devId][varId]
      if cached and cached.value ~= nil and tostring(cached.value) ~= "" then
        local varName = cached.varName or getVariableName(devId, varId)
        tags[varName] = tostring(cached.value)
      end
    end
  end

  -- Determine write interval for this measurement
  local intervalSecs = constants.WRITE_INTERVALS[meas.interval]
    or constants.WRITE_INTERVALS["Default"]
    or self._getWriteInterval()

  influxWriter:enqueue(measName, tags, fields, {
    interval = intervalSecs,
    dedup = true,
  })

  log:debug("Enqueued point for '%s' via influxWriter", measName)
end

--- Callback invoked when a subscribed variable's value changes.
--- Updates the value cache and enqueues data points for all measurements
--- that reference this variable as a field.
--- @param deviceId number
--- @param variableId number
--- @param strValue string
function SubscriptionEngine:_onVariableChanged(deviceId, variableId, strValue)
  log:trace("SubscriptionEngine:_onVariableChanged(%d, %d, '%s')", deviceId, variableId, tostring(strValue))

  -- Update the cache
  self._varCache[deviceId] = self._varCache[deviceId] or {}
  local entry = self._varCache[deviceId][variableId]
  if not entry then
    self._varCache[deviceId][variableId] = {
      value = strValue,
      timestamp = os.time(),
      varName = getVariableName(deviceId, variableId),
    }
  else
    entry.value = strValue
    entry.timestamp = os.time()
  end

  -- During subscription setup, only cache values — don't enqueue yet.
  -- The initial point is enqueued at the end of subscribeToMeasurement
  -- once all fields and tags are cached.
  if self._subscribing then
    return
  end

  -- Enqueue a data point for each measurement that uses this variable as a FIELD
  local varIdStr = string.format("%d:%d", deviceId, variableId)
  local refs = self._subscriptionRefs[varIdStr]
  if refs then
    for measName, kind in pairs(refs) do
      if kind == "fields" then
        self:_enqueueMeasurementPoint(measName)
      end
      -- Tag-only variables update the cache but don't trigger a new point by themselves
    end
  end
end

--- Subscribe to a single variable on behalf of a measurement.
--- Idempotent: if already subscribed, only adds the measurement reference.
--- @param varIdStr string "deviceId:variableId"
--- @param measName string
--- @param kind string "fields" or "tags"
function SubscriptionEngine:_subscribeToVariable(varIdStr, measName, kind)
  log:trace("SubscriptionEngine:_subscribeToVariable('%s', '%s', '%s')", varIdStr, measName, kind)
  local devId, varId = parseVarId(varIdStr)
  if not devId or not varId then
    log:warn("subscribeToVariable: invalid variable ID '%s' for measurement '%s'", varIdStr, measName)
    return
  end

  -- Track the measurement reference
  self._subscriptionRefs[varIdStr] = self._subscriptionRefs[varIdStr] or {}
  self._subscriptionRefs[varIdStr][measName] = kind

  -- Only call RegisterVariableListener once per deviceId:variableId pair
  local refCount = 0
  for _ in pairs(self._subscriptionRefs[varIdStr]) do
    refCount = refCount + 1
  end

  if refCount == 1 then
    -- First subscriber for this variable — register the listener
    -- Wrap the callback to preserve self reference
    RegisterVariableListener(devId, varId, function(dId, vId, val)
      self:_onVariableChanged(dId, vId, val)
    end)
    log:info(
      "Subscribed to variable %s (%s on device %s) for measurement '%s' as %s",
      varIdStr,
      getVariableName(devId, varId),
      getDeviceName(devId),
      measName,
      kind
    )
  else
    log:debug(
      "Variable %s already subscribed (%d refs), added measurement '%s' as %s",
      varIdStr,
      refCount,
      measName,
      kind
    )
  end
end

--- Unregister a variable listener and clear cache if no remaining references.
--- @param varIdStr string
--- @param devId number
--- @param varId number
function SubscriptionEngine:_cleanupVariable(varIdStr, devId, varId)
  log:trace("SubscriptionEngine:_cleanupVariable('%s', %d, %d)", varIdStr, devId, varId)
  UnregisterVariableListener(devId, varId)
  if self._varCache[devId] then
    self._varCache[devId][varId] = nil
    if not next(self._varCache[devId]) then
      self._varCache[devId] = nil
    end
  end
end

---------------------------------------------------------------------------
-- Public API
---------------------------------------------------------------------------

--- Subscribe to all variables for a single measurement.
--- @param measName string
function SubscriptionEngine:subscribeToMeasurement(measName)
  log:trace("SubscriptionEngine:subscribeToMeasurement('%s')", measName)
  local measurements = self._getMeasurements()
  local meas = measurements[measName]
  if not meas then
    return
  end

  if meas.enabled == false then
    log:debug("subscribeToMeasurement('%s'): measurement is disabled, skipping", measName)
    return
  end

  -- Suppress enqueue during subscription setup so we don't write partial
  -- points before all fields and tags are cached.
  self._subscribing = true
  for _, varIdStr in ipairs(meas.fields or {}) do
    self:_subscribeToVariable(varIdStr, measName, "fields")
  end
  for _, varIdStr in ipairs(meas.tags or {}) do
    self:_subscribeToVariable(varIdStr, measName, "tags")
  end
  self._subscribing = false

  -- Enqueue an initial point now that all fields and tags are cached.
  self:_enqueueMeasurementPoint(measName)
end

--- Unsubscribe a measurement from all of its configured variables.
--- If no other measurements reference a variable, the listener is unregistered.
--- @param measName string
function SubscriptionEngine:unsubscribeFromMeasurement(measName)
  log:trace("SubscriptionEngine:unsubscribeFromMeasurement('%s')", measName)
  local measurements = self._getMeasurements()
  local meas = measurements[measName]
  if not meas then
    return
  end

  local allVarIds = {}
  for _, varIdStr in ipairs(meas.fields or {}) do
    allVarIds[varIdStr] = true
  end
  for _, varIdStr in ipairs(meas.tags or {}) do
    allVarIds[varIdStr] = true
  end

  for varIdStr, _ in pairs(allVarIds) do
    local refs = self._subscriptionRefs[varIdStr]
    if refs then
      refs[measName] = nil
      local remaining = 0
      for _ in pairs(refs) do
        remaining = remaining + 1
      end
      if remaining == 0 then
        local devId, varId = parseVarId(varIdStr)
        if devId and varId then
          self:_cleanupVariable(varIdStr, devId, varId)
          log:info("Unsubscribed from variable %s (no remaining measurements)", varIdStr)
        end
        self._subscriptionRefs[varIdStr] = nil
      else
        log:debug("Variable %s still referenced by %d other measurement(s), keeping subscription", varIdStr, remaining)
      end
    end
  end
end

--- Subscribe to all variables across all measurements.
--- Called on driver init to restore subscriptions after a restart.
function SubscriptionEngine:resubscribeAll()
  log:trace("SubscriptionEngine:resubscribeAll()")
  local measurements = self._getMeasurements()
  local count = 0
  for measName, _ in pairs(measurements) do
    self:subscribeToMeasurement(measName)
    count = count + 1
  end
  log:info("resubscribeAll: set up subscriptions for %d measurement(s)", count)
end

--- Handle device removal gracefully: remove all cache entries and log a warning.
--- @param deviceId number
function SubscriptionEngine:handleDeviceRemoved(deviceId)
  log:trace("SubscriptionEngine:handleDeviceRemoved(%d)", deviceId)
  local cleaned = 0
  for varIdStr, refs in pairs(self._subscriptionRefs) do
    local devId, varId = parseVarId(varIdStr)
    if devId == deviceId then
      pcall(UnregisterVariableListener, devId, varId)
      for measName, _ in pairs(refs) do
        log:warn(
          "Device %d removed: variable %s used by measurement '%s' is now unavailable",
          deviceId,
          varIdStr,
          measName
        )
      end
      self._subscriptionRefs[varIdStr] = nil
      cleaned = cleaned + 1
    end
  end
  if self._varCache[deviceId] then
    self._varCache[deviceId] = nil
  end
  if cleaned > 0 then
    log:warn("Cleaned up %d variable subscription(s) for removed device %d", cleaned, deviceId)
  end
end

--- Refresh variable subscriptions for a measurement (e.g. after variables are added/removed).
--- Unsubscribes from any variables no longer in the config, subscribes to new ones.
--- @param measName string
function SubscriptionEngine:refreshSubscriptions(measName)
  log:trace("SubscriptionEngine:refreshSubscriptions('%s')", measName)
  for varIdStr, refs in pairs(self._subscriptionRefs) do
    if refs[measName] then
      refs[measName] = nil
      local remaining = 0
      for _ in pairs(refs) do
        remaining = remaining + 1
      end
      if remaining == 0 then
        local devId, varId = parseVarId(varIdStr)
        if devId and varId then
          self:_cleanupVariable(varIdStr, devId, varId)
        end
        self._subscriptionRefs[varIdStr] = nil
      end
    end
  end
  self:subscribeToMeasurement(measName)
end

return SubscriptionEngine
