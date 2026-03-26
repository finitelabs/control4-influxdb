--- Measurement CRUD and UI management for the InfluxDB Data Logger.
---
--- Handles creating, removing, and configuring measurements. Manages the
--- Composer Pro property UI for measurement selection and variable binding.

local log = require("lib.logging")
local persist = require("lib.persist")
local constants = require("constants")

---------------------------------------------------------------------------
-- Module
---------------------------------------------------------------------------

--- @class MeasurementConfig
--- @field fields string[] Variable IDs for field values.
--- @field tags string[] Variable IDs for tag values.
--- @field interval string Write interval display string (e.g., "Default", "10s").
--- @field enabled boolean Whether this measurement is actively collecting.

--- @class MeasurementManager
--- @field _measurements table<string, MeasurementConfig>
--- @field _selectedMeasurement string|nil
local MeasurementManager = {}
MeasurementManager.__index = MeasurementManager

--- Create a new MeasurementManager instance.
--- Loads saved measurements from persistent storage.
--- @return MeasurementManager
function MeasurementManager:new()
  log:trace("MeasurementManager:new()")
  local instance = setmetatable({}, self)
  instance._measurements = {}
  instance._selectedMeasurement = nil

  -- Load from persistent storage
  local data = persist:get("measurements")
  if data and type(data) == "table" then
    instance._measurements = data
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

--- Return the currently selected measurement name.
--- @return string|nil
function MeasurementManager:getSelected()
  log:trace("MeasurementManager:getSelected()")
  return self._selectedMeasurement
end

--- Set the currently selected measurement and refresh the UI.
--- @param name string|nil
function MeasurementManager:setSelected(name)
  log:trace("MeasurementManager:setSelected('%s')", tostring(name))
  if name == constants.SELECT_OPTION or not name or name == "" then
    self._selectedMeasurement = nil
  else
    self._selectedMeasurement = name
  end
  self:refreshUI()
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
-- CRUD Operations
---------------------------------------------------------------------------

--- Add a new measurement configuration.
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
    fields = {},
    tags = {},
    interval = "Default",
    enabled = true,
  }

  self:_save()
  log:info("Added measurement: %s", name)

  -- Clear the input field and select the new measurement
  UpdateProperty("Add Measurement", "")
  self._selectedMeasurement = name
  self:refreshUI()
  return true
end

--- Remove a measurement configuration.
--- @param name string The measurement name.
--- @param subEngine SubscriptionEngine|nil Subscription engine to unsubscribe variables.
--- @param influxWriter InfluxWriter|nil Writer to remove measurement buffers.
function MeasurementManager:remove(name, subEngine, influxWriter)
  log:trace("MeasurementManager:remove('%s')", name)
  if not self._measurements[name] then
    log:warn("Measurement '%s' does not exist", name)
    return
  end

  -- Unsubscribe from all variables for this measurement before removing it
  if subEngine then
    subEngine:unsubscribeFromMeasurement(name)
  end

  -- Remove from InfluxWriter batch engine (cancels flush timer, drops buffered points)
  if influxWriter then
    influxWriter:removeMeasurement(name)
  end

  self._measurements[name] = nil
  self:_save()
  log:info("Removed measurement: %s", name)

  -- Deselect if the removed measurement was selected
  if self._selectedMeasurement == name then
    self._selectedMeasurement = nil
  end
  self:refreshUI()
end

--- Add a variable to the selected measurement's fields or tags list.
--- @param kind string "fields" or "tags"
--- @param varId string The variable ID (e.g. "96:1005").
--- @param subEngine SubscriptionEngine|nil Subscription engine to subscribe the variable.
--- @return boolean success
function MeasurementManager:addVariable(kind, varId, subEngine)
  log:trace("MeasurementManager:addVariable('%s', '%s')", kind, tostring(varId))
  if not self._selectedMeasurement or not self._measurements[self._selectedMeasurement] then
    log:warn("addVariable: no measurement selected")
    return false
  end
  if IsEmpty(varId) then
    log:warn("addVariable: no variable selected")
    return false
  end

  varId = tostring(varId)
  local meas = self._measurements[self._selectedMeasurement]

  -- Check if already in fields or tags
  for _, existing in ipairs(meas.fields or {}) do
    if tostring(existing) == varId then
      log:warn("Variable '%s' already configured as a field for '%s'", varId, self._selectedMeasurement)
      return false
    end
  end
  for _, existing in ipairs(meas.tags or {}) do
    if tostring(existing) == varId then
      log:warn("Variable '%s' already configured as a tag for '%s'", varId, self._selectedMeasurement)
      return false
    end
  end

  meas[kind] = meas[kind] or {}
  meas[kind][#meas[kind] + 1] = varId

  self:_save()
  log:info(
    "Added variable '%s' as %s to measurement '%s'",
    varId,
    kind == "fields" and "field" or "tag",
    self._selectedMeasurement
  )

  -- Subscribe to the new variable immediately
  if subEngine and meas.enabled ~= false then
    subEngine:_subscribeToVariable(varId, self._selectedMeasurement, kind)
  end

  self:refreshUI()
  return true
end

--- Remove a variable from the selected measurement.
--- @param kind string "fields" or "tags"
--- @param rawValue string The selected value, either a raw ID "96:1005" or label "Name [96:1005]".
--- @param subEngine SubscriptionEngine|nil Subscription engine to refresh subscriptions.
function MeasurementManager:removeVariable(kind, rawValue, subEngine)
  log:trace("MeasurementManager:removeVariable('%s', '%s')", kind, rawValue)
  if not self._selectedMeasurement or not self._measurements[self._selectedMeasurement] then
    log:warn("removeVariable: no measurement selected")
    return
  end

  -- Extract the raw variable ID from a label like "Device > Var [96:1005]"
  local varId = tostring(rawValue):match("%[(%d+:%d+)%]$") or tostring(rawValue)
  local meas = self._measurements[self._selectedMeasurement]

  local newList = {}
  for _, existing in ipairs(meas[kind] or {}) do
    if tostring(existing) ~= varId then
      newList[#newList + 1] = existing
    end
  end
  meas[kind] = newList

  self:_save()
  log:info(
    "Removed %s '%s' from measurement '%s'",
    kind == "fields" and "field" or "tag",
    varId,
    self._selectedMeasurement
  )

  if subEngine then
    subEngine:refreshSubscriptions(self._selectedMeasurement)
  end
  self:refreshUI()
end

--- Update the write interval for the selected measurement.
--- @param interval string
function MeasurementManager:setInterval(interval)
  log:trace("MeasurementManager:setInterval('%s')", interval)
  if not self._selectedMeasurement or not self._measurements[self._selectedMeasurement] then
    return
  end
  local meas = self._measurements[self._selectedMeasurement]
  local previous = meas.interval
  meas.interval = interval
  if previous ~= interval then
    self:_save()
    log:info("Updated write interval for '%s': %s -> %s", self._selectedMeasurement, previous, interval)
  end
end

--- Update the enabled state for the selected measurement.
--- @param enabled boolean
--- @param subEngine SubscriptionEngine|nil Subscription engine for sub/unsub.
function MeasurementManager:setEnabled(enabled, subEngine)
  log:trace("MeasurementManager:setEnabled(%s)", tostring(enabled))
  if not self._selectedMeasurement or not self._measurements[self._selectedMeasurement] then
    return
  end
  local meas = self._measurements[self._selectedMeasurement]
  local previous = meas.enabled
  meas.enabled = enabled
  if previous ~= enabled then
    self:_save()
    log:info(
      "Updated enabled state for '%s': %s -> %s",
      self._selectedMeasurement,
      tostring(previous),
      tostring(enabled)
    )
    if subEngine then
      if enabled then
        subEngine:subscribeToMeasurement(self._selectedMeasurement)
      else
        subEngine:unsubscribeFromMeasurement(self._selectedMeasurement)
      end
    end
  end
end

---------------------------------------------------------------------------
-- UI Helpers
---------------------------------------------------------------------------

--- Resolve a variable ID string to a human-readable label.
--- @param varIdStr string e.g. "96:1005"
--- @return string label e.g. "Shelly BLU Motion > Illuminance"
local function resolveVariableLabel(varIdStr)
  local devStr, varStr = tostring(varIdStr):match("^(%d+):(%d+)$")
  if not devStr then
    return tostring(varIdStr)
  end
  local devId = tonumber(devStr)
  local varId = tonumber(varStr)

  -- Resolve device name
  local devName
  local ok, name = pcall(C4.GetDeviceDisplayName, C4, devId)
  if ok and name and name ~= "" then
    devName = name
  else
    devName = "Device " .. devStr
  end

  -- Resolve variable name
  local varName
  local ok2, vars = pcall(C4.GetDeviceVariables, C4, devId)
  if ok2 and vars then
    local entry = vars[tostring(varId)]
    if entry and entry.name and entry.name ~= "" then
      varName = entry.name
    end
  end
  if not varName then
    varName = "var" .. varStr
  end

  return devName .. " > " .. varName
end

--- Update the "Remove Measurement" and "Configure Measurement" DYNAMIC_LIST properties.
--- Follows the Home Connect pattern: populate both lists with the same options,
--- hide both when there are no measurements.
function MeasurementManager:updateLists()
  log:trace("MeasurementManager:updateLists()")
  UpdateProperty("Add Measurement", "")

  local names = {}
  for name, _ in pairs(self._measurements) do
    names[#names + 1] = name
  end
  table.sort(names)

  -- If the selected measurement no longer exists, fall back to none
  if self._selectedMeasurement and not self._measurements[self._selectedMeasurement] then
    self._selectedMeasurement = nil
  end

  -- Build options list with (Select) placeholder
  local options = { constants.SELECT_OPTION }
  for _, name in ipairs(names) do
    options[#options + 1] = name
  end
  local optionsStr = table.concat(options, ",")

  -- Show/hide based on whether any measurements exist
  local visibility = #names > 0 and constants.SHOW_PROPERTY or constants.HIDE_PROPERTY

  C4:UpdatePropertyList("Remove Measurement", optionsStr, constants.SELECT_OPTION)
  C4:UpdatePropertyList("Configure Measurement", optionsStr, self._selectedMeasurement or constants.SELECT_OPTION)
  C4:SetPropertyAttribs("Remove Measurement", visibility)
  C4:SetPropertyAttribs("Configure Measurement", visibility)

  UpdateProperty("Configure Measurement", self._selectedMeasurement or constants.SELECT_OPTION, true)
  log:debug("Updated measurement lists (%d measurements)", #names)
end

--- Build a DYNAMIC_LIST options string for a variable list with human-readable labels.
--- @param varIds string[] Variable ID strings.
--- @return string optionsStr Comma-delimited options with (Select) placeholder.
--- @return number count Number of variables (excluding placeholder).
local function buildVariableListOptions(varIds)
  local options = { constants.SELECT_OPTION }
  for _, varId in ipairs(varIds) do
    options[#options + 1] = resolveVariableLabel(varId) .. " [" .. varId .. "]"
  end
  return table.concat(options, ","), #varIds
end

--- Refresh measurement config properties to reflect the currently selected measurement.
function MeasurementManager:refreshUI()
  log:trace("MeasurementManager:refreshUI()")
  local configProps = {
    "Add Field",
    "Remove Field",
    "Add Tag",
    "Remove Tag",
    "Measurement Write Interval",
    "Measurement Enabled",
  }

  if not self._selectedMeasurement or not self._measurements[self._selectedMeasurement] then
    for _, propName in ipairs(configProps) do
      C4:SetPropertyAttribs(propName, constants.HIDE_PROPERTY)
    end
    log:debug("refreshMeasurementUI: no measurement selected, hiding config properties")
    return
  end

  local meas = self._measurements[self._selectedMeasurement]

  -- Always show add selectors and settings
  C4:SetPropertyAttribs("Add Field", constants.SHOW_PROPERTY)
  C4:SetPropertyAttribs("Add Tag", constants.SHOW_PROPERTY)
  C4:SetPropertyAttribs("Measurement Write Interval", constants.SHOW_PROPERTY)
  C4:SetPropertyAttribs("Measurement Enabled", constants.SHOW_PROPERTY)

  -- Clear add selectors
  UpdateProperty("Add Field", "")
  UpdateProperty("Add Tag", "")

  -- Populate Remove Field list — hide when empty
  local fieldOpts, fieldCount = buildVariableListOptions(meas.fields or {})
  C4:UpdatePropertyList("Remove Field", fieldOpts, constants.SELECT_OPTION)
  C4:SetPropertyAttribs("Remove Field", fieldCount > 0 and constants.SHOW_PROPERTY or constants.HIDE_PROPERTY)

  -- Populate Remove Tag list — hide when empty
  local tagOpts, tagCount = buildVariableListOptions(meas.tags or {})
  C4:UpdatePropertyList("Remove Tag", tagOpts, constants.SELECT_OPTION)
  C4:SetPropertyAttribs("Remove Tag", tagCount > 0 and constants.SHOW_PROPERTY or constants.HIDE_PROPERTY)

  UpdateProperty("Measurement Write Interval", meas.interval or "Default")
  UpdateProperty("Measurement Enabled", (meas.enabled == false) and "Off" or "On")

  log:debug("refreshMeasurementUI: updated UI for measurement '%s'", self._selectedMeasurement)
end

return MeasurementManager
