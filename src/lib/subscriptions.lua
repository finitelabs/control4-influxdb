--- Variable subscription engine for InfluxDB measurement data collection.
---
--- Manages registering/unregistering Control4 variable listeners, caching
--- variable values, and routing variable changes to the appropriate reading
--- point enqueue calls via InfluxWriter.

local log = require("lib.logging")
local constants = require("constants")
local transform = require("lib.transform")

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

--- Build a set from an array of strings.
--- @param arr string[]
--- @return table<string, boolean>
local function toSet(arr)
  local set = {}
  for _, v in ipairs(arr or {}) do
    set[v] = true
  end
  return set
end

--- Determine if a mapping name is a "field" or "tag" for a measurement.
--- @param meas MeasurementConfig
--- @param mappingName string
--- @return string|nil "field", "tag", or nil if not found
local function resolveKind(meas, mappingName)
  for _, name in ipairs(meas.fieldDefs or {}) do
    if name == mappingName then
      return "field"
    end
  end
  for _, name in ipairs(meas.tagDefs or {}) do
    if name == mappingName then
      return "tag"
    end
  end
  return nil
end

---------------------------------------------------------------------------
-- Module
---------------------------------------------------------------------------

--- @class VarCacheEntry
--- @field value string Current variable value.
--- @field timestamp number Last update timestamp (os.time).
--- @field varName string Human-readable variable name.

--- Subscription ref value stored in _subscriptionRefs[varIdStr][refKey].
--- @class SubscriptionRef
--- @field measName string Measurement name.
--- @field readingLabel string Reading label within the measurement.
--- @field mappingName string Field or tag mapping name.
--- @field kind string "field" or "tag".

--- @class SubscriptionEngine
--- @field _varCache table<number, table<number, VarCacheEntry>>
--- @field _subscriptionRefs table<string, table<string, SubscriptionRef>>
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
  instance._intervalTimers = {} -- {[intervalSecs]: timerName}
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

--- Build and enqueue an InfluxDB data point for a single reading within a
--- measurement. Resolves each mapping (variable or literal), applies
--- transforms, and enqueues via influxWriter.
--- @param measName string
--- @param readingLabel string
function SubscriptionEngine:_enqueueReadingPoint(measName, readingLabel)
  log:trace("SubscriptionEngine:_enqueueReadingPoint('%s', '%s')", measName, readingLabel)
  local measurements = self._getMeasurements()
  local meas = measurements[measName]
  if not meas or meas.enabled == false then
    return
  end

  local reading = meas.readings and meas.readings[readingLabel]
  if not reading or reading.enabled == false then
    return
  end

  local influxWriter = self._getInfluxWriter()
  if not influxWriter then
    log:warn("enqueueReadingPoint: influxWriter not initialized")
    return
  end

  local fieldDefSet = toSet(meas.fieldDefs)
  local tagDefSet = toSet(meas.tagDefs)

  local fields = {}
  local tags = {}

  for mappingName, mapping in pairs(reading.mappings or {}) do
    local rawValue

    if mapping.source == "literal" then
      rawValue = mapping.literal
    elseif mapping.source == "variable" and mapping.varId then
      local devId, varId = parseVarId(mapping.varId)
      if devId and varId then
        local cached = self._varCache[devId] and self._varCache[devId][varId]
        if not cached then
          -- If this is a field mapping, we need it before we can enqueue
          if fieldDefSet[mappingName] then
            log:debug(
              "enqueueReadingPoint(%s::%s): waiting for initial value of %s (field '%s'), skipping",
              measName,
              readingLabel,
              mapping.varId,
              mappingName
            )
            return
          end
          -- Tag mapping without cached value: skip this tag
          rawValue = nil
        else
          rawValue = cached.value
        end
      end
    end

    -- Apply transform if defined
    if rawValue ~= nil and mapping.transform and mapping.transform ~= "" then
      rawValue = transform.eval(mapping.transform, rawValue)
    end

    -- Place into fields or tags based on which def list it belongs to
    if fieldDefSet[mappingName] then
      if rawValue ~= nil then
        fields[mappingName] = {
          value = rawValue,
          type = inferValueType(rawValue),
        }
      end
    elseif tagDefSet[mappingName] then
      if rawValue ~= nil and tostring(rawValue) ~= "" then
        tags[mappingName] = tostring(rawValue)
      end
    end
  end

  if not next(fields) then
    log:debug("enqueueReadingPoint(%s::%s): no fields resolved, skipping", measName, readingLabel)
    return
  end

  -- Determine write interval for this measurement
  local intervalSecs = constants.WRITE_INTERVALS[meas.interval]
    or constants.WRITE_INTERVALS["Default"]
    or self._getWriteInterval()

  local dedupKey = measName .. "::" .. readingLabel

  influxWriter:enqueue(measName, tags, fields, {
    interval = intervalSecs,
    dedup = meas.dedup ~= false,
    dedupKey = dedupKey,
  })

  log:debug("Enqueued point for '%s::%s' via influxWriter", measName, readingLabel)
end

--- Callback invoked when a subscribed variable's value changes.
--- Updates the value cache and enqueues data points for all readings
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
  -- The initial point is enqueued at the end of subscribeToReading
  -- once all fields and tags are cached.
  if self._subscribing then
    return
  end

  -- Enqueue a data point for each reading that uses this variable as a FIELD
  local varIdStr = string.format("%d:%d", deviceId, variableId)
  local refs = self._subscriptionRefs[varIdStr]
  if refs then
    -- Track which readings we've already enqueued to avoid duplicates
    local enqueued = {}
    for _, ref in pairs(refs) do
      if ref.kind == "field" then
        local readingKey = ref.measName .. "::" .. ref.readingLabel
        if not enqueued[readingKey] then
          self:_enqueueReadingPoint(ref.measName, ref.readingLabel)
          enqueued[readingKey] = true
        end
      end
      -- Tag-only variable changes update the cache but don't trigger a new point
    end
  end
end

--- Subscribe to a single variable on behalf of a reading mapping.
--- Idempotent: if already subscribed, only adds the ref.
--- @param varIdStr string "deviceId:variableId"
--- @param measName string
--- @param readingLabel string
--- @param mappingName string
--- @param kind string "field" or "tag"
function SubscriptionEngine:_subscribeToVariable(varIdStr, measName, readingLabel, mappingName, kind)
  log:trace(
    "SubscriptionEngine:_subscribeToVariable('%s', '%s', '%s', '%s', '%s')",
    varIdStr,
    measName,
    readingLabel,
    mappingName,
    kind
  )
  local devId, varId = parseVarId(varIdStr)
  if not devId or not varId then
    log:warn(
      "subscribeToVariable: invalid variable ID '%s' for measurement '%s' reading '%s'",
      varIdStr,
      measName,
      readingLabel
    )
    return
  end

  -- Build refKey: measName::readingLabel::mappingName
  local refKey = measName .. "::" .. readingLabel .. "::" .. mappingName

  -- Track the subscription reference
  self._subscriptionRefs[varIdStr] = self._subscriptionRefs[varIdStr] or {}
  self._subscriptionRefs[varIdStr][refKey] = {
    measName = measName,
    readingLabel = readingLabel,
    mappingName = mappingName,
    kind = kind,
  }

  -- Only call RegisterVariableListener once per deviceId:variableId pair
  local refCount = 0
  for _ in pairs(self._subscriptionRefs[varIdStr]) do
    refCount = refCount + 1
  end

  if refCount == 1 then
    -- First subscriber for this variable — register the listener
    RegisterVariableListener(devId, varId, function(dId, vId, val)
      self:_onVariableChanged(dId, vId, val)
    end)
    log:info(
      "Subscribed to variable %s (%s on device %s) for '%s::%s' mapping '%s' as %s",
      varIdStr,
      getVariableName(devId, varId),
      getDeviceName(devId),
      measName,
      readingLabel,
      mappingName,
      kind
    )
  else
    log:debug(
      "Variable %s already subscribed (%d refs), added '%s::%s' mapping '%s' as %s",
      varIdStr,
      refCount,
      measName,
      readingLabel,
      mappingName,
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

--- Subscribe to all variable-source mappings for a single reading.
--- @param measName string
--- @param readingLabel string
function SubscriptionEngine:subscribeToReading(measName, readingLabel)
  log:trace("SubscriptionEngine:subscribeToReading('%s', '%s')", measName, readingLabel)
  local measurements = self._getMeasurements()
  local meas = measurements[measName]
  if not meas then
    return
  end

  if meas.enabled == false then
    log:debug("subscribeToReading('%s', '%s'): measurement is disabled, skipping", measName, readingLabel)
    return
  end

  local reading = meas.readings and meas.readings[readingLabel]
  if not reading then
    log:debug("subscribeToReading('%s', '%s'): reading not found, skipping", measName, readingLabel)
    return
  end

  if reading.enabled == false then
    log:debug("subscribeToReading('%s', '%s'): reading is disabled, skipping", measName, readingLabel)
    return
  end

  -- Suppress enqueue during subscription setup so we don't write partial
  -- points before all fields and tags are cached.
  self._subscribing = true
  for mappingName, mapping in pairs(reading.mappings or {}) do
    if mapping.source == "variable" and mapping.varId then
      local kind = resolveKind(meas, mappingName)
      if kind then
        self:_subscribeToVariable(mapping.varId, measName, readingLabel, mappingName, kind)
      else
        log:warn(
          "subscribeToReading('%s', '%s'): mapping '%s' not in fieldDefs or tagDefs, skipping",
          measName,
          readingLabel,
          mappingName
        )
      end
    end
  end
  self._subscribing = false

  -- Enqueue an initial point now that all fields and tags are cached.
  self:_enqueueReadingPoint(measName, readingLabel)
end

--- Unsubscribe a single reading from all of its variable-source mappings.
--- If no other refs reference a variable, the listener is unregistered.
--- @param measName string
--- @param readingLabel string
function SubscriptionEngine:unsubscribeFromReading(measName, readingLabel)
  log:trace("SubscriptionEngine:unsubscribeFromReading('%s', '%s')", measName, readingLabel)

  -- Build the prefix to match refKeys belonging to this reading
  local refPrefix = measName .. "::" .. readingLabel .. "::"

  for varIdStr, refs in pairs(self._subscriptionRefs) do
    local toRemove = {}
    for refKey, _ in pairs(refs) do
      if refKey:sub(1, #refPrefix) == refPrefix then
        toRemove[#toRemove + 1] = refKey
      end
    end

    for _, refKey in ipairs(toRemove) do
      refs[refKey] = nil
    end

    -- If no remaining refs for this variable, clean up
    if not next(refs) then
      local devId, varId = parseVarId(varIdStr)
      if devId and varId then
        self:_cleanupVariable(varIdStr, devId, varId)
        log:info("Unsubscribed from variable %s (no remaining refs)", varIdStr)
      end
      self._subscriptionRefs[varIdStr] = nil
    end
  end
end

--- Refresh subscriptions for a single reading (unsubscribe then resubscribe).
--- @param measName string
--- @param readingLabel string
function SubscriptionEngine:refreshReadingSubscriptions(measName, readingLabel)
  log:trace("SubscriptionEngine:refreshReadingSubscriptions('%s', '%s')", measName, readingLabel)
  self:unsubscribeFromReading(measName, readingLabel)
  self:subscribeToReading(measName, readingLabel)
end

--- Subscribe to all variables for a single measurement (all enabled readings).
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

  for readingLabel, reading in pairs(meas.readings or {}) do
    if reading.enabled ~= false then
      self:subscribeToReading(measName, readingLabel)
    end
  end
end

--- Unsubscribe a measurement from all of its readings' variables.
--- If no other refs reference a variable, the listener is unregistered.
--- @param measName string
function SubscriptionEngine:unsubscribeFromMeasurement(measName)
  log:trace("SubscriptionEngine:unsubscribeFromMeasurement('%s')", measName)
  local measurements = self._getMeasurements()
  local meas = measurements[measName]
  if not meas then
    return
  end

  for readingLabel, _ in pairs(meas.readings or {}) do
    self:unsubscribeFromReading(measName, readingLabel)
  end
end

--- Refresh subscriptions for all readings in a measurement (unsubscribe then resubscribe).
--- @param measName string
function SubscriptionEngine:refreshMeasurementSubscriptions(measName)
  log:trace("SubscriptionEngine:refreshMeasurementSubscriptions('%s')", measName)
  self:unsubscribeFromMeasurement(measName)
  self:subscribeToMeasurement(measName)
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
  self:_startIntervalTimers()
end

--- Start global interval timers grouped by write interval.
--- One timer per distinct interval; each tick re-enqueues all readings at that interval.
function SubscriptionEngine:_startIntervalTimers()
  self:_stopIntervalTimers()
  local measurements = self._getMeasurements()
  -- Group readings by interval
  local byInterval = {} -- {[secs]: {{measName, readingLabel}, ...}}
  for measName, meas in pairs(measurements) do
    if meas.enabled ~= false then
      local intervalSecs = constants.WRITE_INTERVALS[meas.interval]
        or constants.WRITE_INTERVALS["Default"]
        or self._getWriteInterval()
      byInterval[intervalSecs] = byInterval[intervalSecs] or {}
      for readingLabel, reading in pairs(meas.readings or {}) do
        if reading.enabled ~= false then
          byInterval[intervalSecs][#byInterval[intervalSecs] + 1] = {
            measName = measName,
            readingLabel = readingLabel,
          }
        end
      end
    end
  end
  -- Create one repeating timer per interval
  for secs, readings in pairs(byInterval) do
    if #readings > 0 then
      local timerName = "InfluxInterval_" .. secs
      self._intervalTimers[secs] = timerName
      SetTimer(timerName, secs * 1000, function()
        for _, r in ipairs(readings) do
          self:_enqueueReadingPoint(r.measName, r.readingLabel)
        end
      end, true) -- true = repeating
      log:info("Started %ds interval timer for %d reading(s)", secs, #readings)
    end
  end
end

--- Stop all global interval timers.
function SubscriptionEngine:_stopIntervalTimers()
  for secs, timerName in pairs(self._intervalTimers) do
    pcall(CancelTimer, timerName)
  end
  self._intervalTimers = {}
end

--- Restart interval timers (call after measurement config changes).
function SubscriptionEngine:restartIntervalTimers()
  self:_startIntervalTimers()
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
      for refKey, ref in pairs(refs) do
        log:warn(
          "Device %d removed: variable %s used by '%s::%s' mapping '%s' is now unavailable",
          deviceId,
          varIdStr,
          ref.measName,
          ref.readingLabel,
          ref.mappingName
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

--- Refresh variable subscriptions for a measurement (legacy compatibility).
--- Unsubscribes from any variables no longer in the config, subscribes to new ones.
--- @param measName string
function SubscriptionEngine:refreshSubscriptions(measName)
  log:trace("SubscriptionEngine:refreshSubscriptions('%s')", measName)
  self:refreshMeasurementSubscriptions(measName)
end

return SubscriptionEngine
