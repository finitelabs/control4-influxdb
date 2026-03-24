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

--- The currently selected measurement name (nil if no measurement is selected).
--- @type string|nil
local selectedMeasurement = nil

--- Driver initialization flag.
--- @type boolean
local gInitialized = false

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

--- Refresh the "Select Measurement" DYNAMIC_LIST property with current measurement names.
--- Follows the Home Connect pattern: show the list when options exist, hide when empty.
local function refreshMeasurementList()
  -- Build sorted list of measurement names
  local names = {}
  for name, _ in pairs(measurements) do
    names[#names + 1] = name
  end
  table.sort(names)

  -- If the selected measurement no longer exists, fall back to none
  if selectedMeasurement and not measurements[selectedMeasurement] then
    selectedMeasurement = nil
  end

  if #names > 0 then
    -- Build comma-delimited options list
    local options = { constants.SELECT_OPTION }
    for _, name in ipairs(names) do
      options[#options + 1] = name
    end
    C4:UpdatePropertyList(
      "Select Measurement",
      table.concat(options, ","),
      selectedMeasurement or constants.SELECT_OPTION
    )
    C4:SetPropertyAttribs("Select Measurement", constants.SHOW_PROPERTY)
  else
    C4:SetPropertyAttribs("Select Measurement", constants.HIDE_PROPERTY)
  end

  -- Reset the selection display
  UpdateProperty("Select Measurement", selectedMeasurement or constants.SELECT_OPTION, true)
  log:debug("Refreshed measurement list (%d measurements)", #names)
end

--- Build a display string for the configured variables of the selected measurement.
--- @param meas table The measurement config table.
--- @return string display
local function buildConfiguredVariablesDisplay(meas)
  local fieldStrs = {}
  for _, varId in ipairs(meas.fields or {}) do
    fieldStrs[#fieldStrs + 1] = tostring(varId)
  end

  local tagStrs = {}
  for _, varId in ipairs(meas.tags or {}) do
    tagStrs[#tagStrs + 1] = tostring(varId)
  end

  local parts = {}
  if #fieldStrs > 0 then
    parts[#parts + 1] = "Fields: " .. table.concat(fieldStrs, ", ")
  end
  if #tagStrs > 0 then
    parts[#parts + 1] = "Tags: " .. table.concat(tagStrs, ", ")
  end

  if #parts == 0 then
    return "(none configured)"
  end
  return table.concat(parts, " | ")
end

--- Refresh measurement config properties to reflect the currently selected measurement.
--- Follows the Home Connect "Configure Camera" pattern: show/hide config properties
--- based on whether a valid measurement is selected in the context dropdown.
local function refreshMeasurementUI()
  --- Properties that are only visible when a measurement is selected.
  local configProps = {
    "Select Variable",
    "Configured Variables",
    "Remove Variable",
    "Measurement Write Interval",
    "Measurement Enabled",
  }

  if not selectedMeasurement or not measurements[selectedMeasurement] then
    -- Hide all measurement config properties
    for _, propName in ipairs(configProps) do
      C4:SetPropertyAttribs(propName, constants.HIDE_PROPERTY)
    end
    log:debug("refreshMeasurementUI: no measurement selected, hiding config properties")
    return
  end

  -- Show measurement config properties
  for _, propName in ipairs(configProps) do
    C4:SetPropertyAttribs(propName, constants.SHOW_PROPERTY)
  end

  local meas = measurements[selectedMeasurement]

  -- Populate "Configured Variables" read-only display
  local display = buildConfiguredVariablesDisplay(meas)
  UpdateProperty("Configured Variables", display)

  -- Populate "Remove Variable" dynamic list with all variable IDs for this measurement
  local allVars = {}
  for _, varId in ipairs(meas.fields or {}) do
    allVars[#allVars + 1] = tostring(varId)
  end
  for _, varId in ipairs(meas.tags or {}) do
    allVars[#allVars + 1] = tostring(varId)
  end

  if #allVars > 0 then
    local options = { constants.SELECT_OPTION }
    for _, varId in ipairs(allVars) do
      options[#options + 1] = varId
    end
    C4:UpdatePropertyList("Remove Variable", table.concat(options, ","), constants.SELECT_OPTION)
    C4:SetPropertyAttribs("Remove Variable", constants.SHOW_PROPERTY)
  else
    C4:SetPropertyAttribs("Remove Variable", constants.HIDE_PROPERTY)
  end

  -- Set write interval
  local interval = meas.interval or "Default"
  UpdateProperty("Measurement Write Interval", interval)

  -- Set enabled state
  local enabledStr = (meas.enabled == false) and "Off" or "On"
  UpdateProperty("Measurement Enabled", enabledStr)

  log:debug("refreshMeasurementUI: updated UI for measurement '%s'", selectedMeasurement)
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
    interval = "Default",
    enabled = true,
  }

  saveMeasurements()
  log:info("Added measurement: %s", name)

  -- Select the new measurement and refresh UI
  selectedMeasurement = name
  refreshMeasurementList()
  refreshMeasurementUI()
end

--- Remove a measurement configuration.
--- @param name string The measurement name.
local function removeMeasurement(name)
  if not measurements[name] then
    log:warning("Measurement '%s' does not exist", name)
    return
  end

  -- TODO (DRV-9): Unsubscribe from variables for this measurement

  measurements[name] = nil
  saveMeasurements()
  log:info("Removed measurement: %s", name)

  -- Deselect and refresh
  selectedMeasurement = nil
  refreshMeasurementList()
  refreshMeasurementUI()
end

--- Add a variable to a measurement's fields or tags list.
--- @param kind string "fields" or "tags"
local function addVariable(kind)
  if not selectedMeasurement or not measurements[selectedMeasurement] then
    log:warning("addVariable: no measurement selected")
    return
  end

  local varId = Properties["Select Variable"]
  if not varId or varId == "" then
    log:warning("addVariable: no variable selected")
    return
  end

  varId = tostring(varId)
  local meas = measurements[selectedMeasurement]

  -- Check if already in fields or tags
  for _, existing in ipairs(meas.fields or {}) do
    if tostring(existing) == varId then
      log:warning("Variable '%s' already configured as a field for '%s'", varId, selectedMeasurement)
      return
    end
  end
  for _, existing in ipairs(meas.tags or {}) do
    if tostring(existing) == varId then
      log:warning("Variable '%s' already configured as a tag for '%s'", varId, selectedMeasurement)
      return
    end
  end

  meas[kind] = meas[kind] or {}
  meas[kind][#meas[kind] + 1] = varId

  saveMeasurements()
  log:info(
    "Added variable '%s' as %s to measurement '%s'",
    varId,
    kind == "fields" and "field" or "tag",
    selectedMeasurement
  )

  refreshMeasurementUI()
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

--- Handle the "Select Measurement" property change.
--- Updates visibility of configuration properties based on the selected measurement.
--- @param propertyValue string
function OPC.Select_Measurement(propertyValue)
  log:trace("OPC.Select_Measurement('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if propertyValue == constants.SELECT_OPTION or not propertyValue or propertyValue == "" then
    selectedMeasurement = nil
  else
    selectedMeasurement = propertyValue
  end
  refreshMeasurementUI()
end

--- Handle the "Measurement Write Interval" property change for the selected measurement.
--- @param propertyValue string
function OPC.Measurement_Write_Interval(propertyValue)
  log:trace("OPC.Measurement_Write_Interval('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if selectedMeasurement and measurements[selectedMeasurement] then
    local previous = measurements[selectedMeasurement].interval
    measurements[selectedMeasurement].interval = propertyValue
    if previous ~= propertyValue then
      saveMeasurements()
      log:info("Updated write interval for '%s': %s -> %s", selectedMeasurement, previous, propertyValue)
    end
  end
end

--- Handle the "Measurement Enabled" property change for the selected measurement.
--- @param propertyValue string
function OPC.Measurement_Enabled(propertyValue)
  log:trace("OPC.Measurement_Enabled('%s')", propertyValue)
  if not gInitialized then
    return
  end
  if selectedMeasurement and measurements[selectedMeasurement] then
    local enabled = (propertyValue == "On")
    local previous = measurements[selectedMeasurement].enabled
    measurements[selectedMeasurement].enabled = enabled
    if previous ~= enabled then
      saveMeasurements()
      log:info("Updated enabled state for '%s': %s -> %s", selectedMeasurement, tostring(previous), tostring(enabled))
    end
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

--- Add as Field action handler.
function EC.AddAsField()
  log:info("Action: Add as Field")
  addVariable("fields")
end

--- Add as Tag action handler.
function EC.AddAsTag()
  log:info("Action: Add as Tag")
  addVariable("tags")
end

--- Remove Selected Variable action handler.
function EC.RemoveSelectedVariable()
  log:info("Action: Remove Selected Variable")
  if not selectedMeasurement or not measurements[selectedMeasurement] then
    log:warning("RemoveSelectedVariable: no measurement selected")
    return
  end

  local varId = Properties["Remove Variable"]
  if not varId or varId == "" or varId == constants.SELECT_OPTION then
    log:warning("RemoveSelectedVariable: no variable selected")
    return
  end

  varId = tostring(varId)
  local meas = measurements[selectedMeasurement]

  -- Remove from fields
  local newFields = {}
  for _, existing in ipairs(meas.fields or {}) do
    if tostring(existing) ~= varId then
      newFields[#newFields + 1] = existing
    end
  end
  meas.fields = newFields

  -- Remove from tags
  local newTags = {}
  for _, existing in ipairs(meas.tags or {}) do
    if tostring(existing) ~= varId then
      newTags[#newTags + 1] = existing
    end
  end
  meas.tags = newTags

  saveMeasurements()
  log:info("Removed variable '%s' from measurement '%s'", varId, selectedMeasurement)
  refreshMeasurementUI()
end

--- Remove Measurement action handler.
function EC.RemoveMeasurement()
  log:info("Action: Remove Measurement")
  if not selectedMeasurement then
    log:warning("RemoveMeasurement: no measurement selected")
    return
  end
  removeMeasurement(selectedMeasurement)
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

  -- Refresh measurement list and hide per-measurement config properties until one is selected
  refreshMeasurementList()
  refreshMeasurementUI()

  -- Fire OnPropertyChanged for all properties to ensure consistent state
  for p, _ in pairs(Properties) do
    local status, err = pcall(OnPropertyChanged, p)
    if not status then
      log:error(tostring(err))
    end
  end

  -- Start with disconnected status until explicitly tested
  updateConnectionStatus(false, "Not tested")

  gInitialized = true
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
