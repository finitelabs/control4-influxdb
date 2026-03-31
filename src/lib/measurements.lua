--- Measurement schema and reading management for the InfluxDB Data Logger.
---
--- Handles creating, removing, and configuring measurements with schema-based
--- field/tag definitions and per-device readings. Configuration is managed
--- through the web UI tab, not Composer Pro properties.

local log = require("lib.logging")
local persist = require("lib.persist")

---------------------------------------------------------------------------
-- Module
---------------------------------------------------------------------------

--- @class MappingDef
--- @field varId string|nil "deviceId:variableId" for variable source.
--- @field source string "variable" | "literal"
--- @field transform string|nil Lua expression, e.g. "value * 100".
--- @field literal string|nil Fixed value when source=="literal".

--- @class ReadingDef
--- @field enabled boolean
--- @field mappings table<string, MappingDef> fieldName/tagName -> mapping.

--- @class MeasurementConfig
--- @field fieldDefs string[] Ordered field names, e.g. {"level", "voltage"}.
--- @field tagDefs string[] Ordered tag names, e.g. {"device_name"}.
--- @field readings table<string, ReadingDef> label -> reading definition.
--- @field interval string Write interval display string.
--- @field enabled boolean Whether this measurement is actively collecting.
--- @field dedup boolean Whether to skip writes when field values are unchanged (default true).

--- @class MeasurementManager
--- @field _measurements table<string, MeasurementConfig>
local MeasurementManager = {}
MeasurementManager.__index = MeasurementManager

--- Create a new MeasurementManager instance.
--- Loads saved measurements from persistent storage.
--- @return MeasurementManager
function MeasurementManager:new()
  log:trace("MeasurementManager:new()")
  local instance = setmetatable({}, self)
  instance._measurements = {}

  local data = persist:get("measurements")
  if data and type(data) == "table" then
    -- Only load new-format configs (have fieldDefs)
    for name, meas in pairs(data) do
      if meas.fieldDefs then
        instance._measurements[name] = meas
      else
        log:info("Discarding old-format measurement '%s'", name)
      end
    end
    local count = 0
    for _ in pairs(instance._measurements) do
      count = count + 1
    end
    log:info("Loaded %d measurement(s) from storage", count)
  end

  return instance
end

---------------------------------------------------------------------------
-- Accessors
---------------------------------------------------------------------------

--- Return the measurements table reference.
--- @return table<string, MeasurementConfig>
function MeasurementManager:getAll()
  log:trace("MeasurementManager:getAll()")
  return self._measurements
end

--- Return a single measurement config.
--- @param name string
--- @return MeasurementConfig|nil
function MeasurementManager:get(name)
  log:trace("MeasurementManager:get('%s')", name)
  return self._measurements[name]
end

---------------------------------------------------------------------------
-- Persistence
---------------------------------------------------------------------------

--- Save measurement configurations to persistent storage.
function MeasurementManager:_save()
  log:trace("MeasurementManager:_save()")
  persist:set("measurements", self._measurements)
end

---------------------------------------------------------------------------
-- Measurement CRUD
---------------------------------------------------------------------------

--- Add a new measurement.
--- @param name string The measurement name.
--- @return boolean success
function MeasurementManager:add(name)
  log:trace("MeasurementManager:add('%s')", tostring(name))
  if not name or name == "" then
    log:warn("Cannot add measurement: name is empty")
    return false
  end

  -- Sanitize: InfluxDB measurement names should not contain spaces or commas
  name = name:gsub("[%s,]", "_")

  if self._measurements[name] then
    log:warn("Measurement '%s' already exists", name)
    return false
  end

  self._measurements[name] = {
    fieldDefs = {},
    tagDefs = {},
    readings = {},
    interval = "Default",
    enabled = true,
  }

  self:_save()
  log:info("Added measurement: %s", name)
  return true
end

--- Remove a measurement.
--- @param name string
--- @param subEngine SubscriptionEngine|nil
--- @param influxWriter InfluxWriter|nil
function MeasurementManager:remove(name, subEngine, influxWriter)
  log:trace("MeasurementManager:remove('%s')", name)
  if not self._measurements[name] then
    log:warn("Measurement '%s' does not exist", name)
    return
  end

  if subEngine then
    subEngine:unsubscribeFromMeasurement(name)
  end

  if influxWriter then
    -- Remove all reading-keyed buffers for this measurement
    local meas = self._measurements[name]
    for readingLabel in pairs(meas.readings or {}) do
      influxWriter:removeMeasurement(name .. "::" .. readingLabel)
    end
  end

  self._measurements[name] = nil
  self:_save()
  log:info("Removed measurement: %s", name)
end

--- Update measurement settings.
--- @param name string
--- @param settings table Partial settings to merge: {interval?, enabled?, dedup?}
--- @param subEngine SubscriptionEngine|nil
function MeasurementManager:updateSettings(name, settings, subEngine)
  log:trace("MeasurementManager:updateSettings('%s')", name)
  local meas = self._measurements[name]
  if not meas then
    return
  end

  if settings.interval ~= nil then
    meas.interval = settings.interval
  end

  if settings.dedup ~= nil then
    meas.dedup = settings.dedup
  end

  if settings.enabled ~= nil and settings.enabled ~= meas.enabled then
    meas.enabled = settings.enabled
    if subEngine then
      if settings.enabled then
        subEngine:subscribeToMeasurement(name)
      else
        subEngine:unsubscribeFromMeasurement(name)
      end
    end
  end

  self:_save()
end

---------------------------------------------------------------------------
-- Schema CRUD (Field/Tag Definitions)
---------------------------------------------------------------------------

--- Add a field name to the measurement schema.
--- @param measName string
--- @param fieldName string
--- @return boolean success
function MeasurementManager:addFieldDef(measName, fieldName)
  log:trace("MeasurementManager:addFieldDef('%s', '%s')", measName, fieldName)
  local meas = self._measurements[measName]
  if not meas then
    return false
  end

  -- Check for duplicates in fields and tags
  for _, existing in ipairs(meas.fieldDefs) do
    if existing == fieldName then
      log:warn("Field '%s' already exists in measurement '%s'", fieldName, measName)
      return false
    end
  end
  for _, existing in ipairs(meas.tagDefs) do
    if existing == fieldName then
      log:warn("'%s' already exists as a tag in measurement '%s'", fieldName, measName)
      return false
    end
  end

  meas.fieldDefs[#meas.fieldDefs + 1] = fieldName
  self:_save()
  log:info("Added field def '%s' to measurement '%s'", fieldName, measName)
  return true
end

--- Remove a field name from the measurement schema.
--- Also removes the corresponding mapping from all readings.
--- @param measName string
--- @param fieldName string
--- @param subEngine SubscriptionEngine|nil
function MeasurementManager:removeFieldDef(measName, fieldName, subEngine)
  log:trace("MeasurementManager:removeFieldDef('%s', '%s')", measName, fieldName)
  local meas = self._measurements[measName]
  if not meas then
    return
  end

  local newDefs = {}
  for _, def in ipairs(meas.fieldDefs) do
    if def ~= fieldName then
      newDefs[#newDefs + 1] = def
    end
  end
  meas.fieldDefs = newDefs

  -- Remove from all readings
  for _, reading in pairs(meas.readings) do
    reading.mappings[fieldName] = nil
  end

  self:_save()
  log:info("Removed field def '%s' from measurement '%s'", fieldName, measName)

  if subEngine then
    subEngine:refreshMeasurementSubscriptions(measName)
  end
end

--- Add a tag name to the measurement schema.
--- @param measName string
--- @param tagName string
--- @return boolean success
function MeasurementManager:addTagDef(measName, tagName)
  log:trace("MeasurementManager:addTagDef('%s', '%s')", measName, tagName)
  local meas = self._measurements[measName]
  if not meas then
    return false
  end

  for _, existing in ipairs(meas.tagDefs) do
    if existing == tagName then
      log:warn("Tag '%s' already exists in measurement '%s'", tagName, measName)
      return false
    end
  end
  for _, existing in ipairs(meas.fieldDefs) do
    if existing == tagName then
      log:warn("'%s' already exists as a field in measurement '%s'", tagName, measName)
      return false
    end
  end

  meas.tagDefs[#meas.tagDefs + 1] = tagName
  self:_save()
  log:info("Added tag def '%s' to measurement '%s'", tagName, measName)
  return true
end

--- Remove a tag name from the measurement schema.
--- Also removes the corresponding mapping from all readings.
--- @param measName string
--- @param tagName string
--- @param subEngine SubscriptionEngine|nil
function MeasurementManager:removeTagDef(measName, tagName, subEngine)
  log:trace("MeasurementManager:removeTagDef('%s', '%s')", measName, tagName)
  local meas = self._measurements[measName]
  if not meas then
    return
  end

  local newDefs = {}
  for _, def in ipairs(meas.tagDefs) do
    if def ~= tagName then
      newDefs[#newDefs + 1] = def
    end
  end
  meas.tagDefs = newDefs

  for _, reading in pairs(meas.readings) do
    reading.mappings[tagName] = nil
  end

  self:_save()
  log:info("Removed tag def '%s' from measurement '%s'", tagName, measName)

  if subEngine then
    subEngine:refreshMeasurementSubscriptions(measName)
  end
end

---------------------------------------------------------------------------
-- Reading CRUD
---------------------------------------------------------------------------

--- Add a reading to a measurement.
--- @param measName string
--- @param label string Human-readable label for the reading.
--- @return boolean success
function MeasurementManager:addReading(measName, label)
  log:trace("MeasurementManager:addReading('%s', '%s')", measName, label)
  local meas = self._measurements[measName]
  if not meas then
    return false
  end

  if not label or label == "" then
    log:warn("Cannot add reading: label is empty")
    return false
  end

  if meas.readings[label] then
    log:warn("Reading '%s' already exists in measurement '%s'", label, measName)
    return false
  end

  meas.readings[label] = {
    enabled = true,
    mappings = {},
  }

  self:_save()
  log:info("Added reading '%s' to measurement '%s'", label, measName)
  return true
end

--- Remove a reading from a measurement.
--- @param measName string
--- @param label string
--- @param subEngine SubscriptionEngine|nil
--- @param influxWriter InfluxWriter|nil
function MeasurementManager:removeReading(measName, label, subEngine, influxWriter)
  log:trace("MeasurementManager:removeReading('%s', '%s')", measName, label)
  local meas = self._measurements[measName]
  if not meas or not meas.readings[label] then
    return
  end

  if subEngine then
    subEngine:unsubscribeFromReading(measName, label)
  end

  if influxWriter then
    influxWriter:removeMeasurement(measName .. "::" .. label)
  end

  meas.readings[label] = nil
  self:_save()
  log:info("Removed reading '%s' from measurement '%s'", label, measName)
end

--- Update reading enabled state.
--- @param measName string
--- @param label string
--- @param enabled boolean
--- @param subEngine SubscriptionEngine|nil
function MeasurementManager:setReadingEnabled(measName, label, enabled, subEngine)
  local meas = self._measurements[measName]
  if not meas or not meas.readings[label] then
    return
  end

  local reading = meas.readings[label]
  if reading.enabled == enabled then
    return
  end

  reading.enabled = enabled
  self:_save()

  if subEngine then
    if enabled then
      subEngine:subscribeToReading(measName, label)
    else
      subEngine:unsubscribeFromReading(measName, label)
    end
  end
end

---------------------------------------------------------------------------
-- Mapping CRUD
---------------------------------------------------------------------------

--- Set or update a mapping for a reading.
--- @param measName string
--- @param readingLabel string
--- @param mappingName string The field or tag name being mapped.
--- @param mappingDef MappingDef
--- @param subEngine SubscriptionEngine|nil
function MeasurementManager:setMapping(measName, readingLabel, mappingName, mappingDef, subEngine)
  log:trace("MeasurementManager:setMapping('%s', '%s', '%s')", measName, readingLabel, mappingName)
  local meas = self._measurements[measName]
  if not meas or not meas.readings[readingLabel] then
    return
  end

  local reading = meas.readings[readingLabel]
  reading.mappings[mappingName] = mappingDef

  self:_save()
  log:info("Set mapping '%s' for reading '%s' in measurement '%s'", mappingName, readingLabel, measName)

  if subEngine and meas.enabled and reading.enabled then
    subEngine:refreshReadingSubscriptions(measName, readingLabel)
  end
end

--- Remove a mapping from a reading.
--- @param measName string
--- @param readingLabel string
--- @param mappingName string
--- @param subEngine SubscriptionEngine|nil
function MeasurementManager:removeMapping(measName, readingLabel, mappingName, subEngine)
  log:trace("MeasurementManager:removeMapping('%s', '%s', '%s')", measName, readingLabel, mappingName)
  local meas = self._measurements[measName]
  if not meas or not meas.readings[readingLabel] then
    return
  end

  meas.readings[readingLabel].mappings[mappingName] = nil
  self:_save()

  if subEngine then
    subEngine:refreshReadingSubscriptions(measName, readingLabel)
  end
end

---------------------------------------------------------------------------
-- JSON Serialization (for Web UI)
---------------------------------------------------------------------------

--- Return the full measurement config as a JSON-serializable table.
--- @return table
function MeasurementManager:getConfigData()
  log:trace("MeasurementManager:getConfigData()")
  return self._measurements
end

--- Apply a complete measurement config received from the web UI.
--- @param measName string
--- @param config MeasurementConfig
--- @param subEngine SubscriptionEngine|nil
--- @param influxWriter InfluxWriter|nil
function MeasurementManager:applyMeasurementConfig(measName, config, subEngine, influxWriter)
  log:trace("MeasurementManager:applyMeasurementConfig('%s')", measName)

  -- Unsubscribe from old config
  if self._measurements[measName] and subEngine then
    subEngine:unsubscribeFromMeasurement(measName)
  end

  -- Remove old writer buffers
  if self._measurements[measName] and influxWriter then
    for readingLabel in pairs(self._measurements[measName].readings or {}) do
      influxWriter:removeMeasurement(measName .. "::" .. readingLabel)
    end
  end

  self._measurements[measName] = config
  self:_save()

  -- Subscribe to new config
  if subEngine and config.enabled then
    subEngine:subscribeToMeasurement(measName)
  end

  log:info("Applied config for measurement '%s'", measName)
end

return MeasurementManager
