-- The Status tab's Properties card and status card, in the real driver: the properties the tab is sent, a change made
-- from it, the changes it is turned away from, a change made in Composer reaching it, the status it shows, and the
-- tab bar that shows only with two or more tabs.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_tab_properties.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

local posts = common.boot({ AC = 1, AS = "License Activated" }, 999)

local ROOT = (debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "../"

local pushes = {}
function C4:SendDataToUI(command, data)
  pushes[#pushes + 1] = { command = command, data = data }
end

local function data(t)
  return { DATA = C4:Base64Encode(JSON:encode(t)) }
end

local function set(name, value)
  return JSON:decode(UIR._SET_PROPERTY(data({ property = name, value = value })))
end

local function shown()
  return JSON:decode(JSON:decode(UIR._GET_CONFIG()).properties)
end

local function property(name)
  for _, p in ipairs(shown()) do
    if p.name == name then
      return p
    end
  end
end

local function pushed(command, since)
  local n = 0
  for i = (since or 0) + 1, #pushes do
    if pushes[i].command == command then
      n = n + 1
    end
  end
  return n
end

local watts = 100

--- A reading changes and its write goes out.
local function write()
  watts = watts + 1
  OnWatchedVariableChanged(common.SENSOR, 1000, tostring(watts))
  cloud.poll(1)
  ShimFireTimers()
end

write()

T.section("What the tab is sent")
do
  local names = {}
  for _, p in ipairs(shown()) do
    names[p.name] = p
  end
  T.eq(
    "the InfluxDB URL, under its Composer label",
    names["InfluxDB URL"] and names["InfluxDB URL"].group,
    "InfluxDB Connection"
  )
  T.eq(
    "the outage threshold, under its label",
    names["Outage Notification Threshold"].group,
    "Offline Buffer and Retry"
  )
  T.eq("API Token stays in Composer", names["API Token"], nil)
  T.excludes("and its value never reaches the tab's config", UIR._GET_CONFIG(), "throwaway")
  T.excludes("or its status", UIR._GET_STATUS(), "throwaway")
  T.eq("and so does the Support Code", names["Support Code"], nil)
  T.eq("Driver Status is read-only", names["Driver Status"].readonly, true)
  T.eq("Write Precision offers its choices", names["Write Precision"].choices, { "ns", "us", "ms", "s" })
  T.truthy("Log Level offers the logger's levels", #(names["Log Level"].choices or {}) >= 4)
  local missing = {}
  for name, p in pairs(names) do
    local own = p.group == "InfluxDB Connection" or p.group == "Offline Buffer and Retry"
    if own and (not p.description or p.description == "" or #p.description > 80) then
      missing[#missing + 1] = name
    end
  end
  T.eq("every InfluxDB setting has a short description", missing, {})
  -- Composer takes an & as a Windows accelerator and drops it from what it shows.
  local fh = assert(io.open(ROOT .. "drivers/influxdb/driver.xml", "r"))
  local xml = fh:read("*a")
  fh:close()
  local shown = {}
  for tag, text in xml:gmatch("<(%a+)>([^<]*&amp;[^<]*)</%a+>") do
    if tag == "name" or tag == "default" or tag == "item" or tag == "description" or tag == "display_name" then
      shown[#shown + 1] = text
    end
  end
  T.eq("no text Composer shows holds an &", shown, {})
end

T.section("A change from the tab")
do
  local mark = #posts
  local reply = set("Write Precision", "s")
  T.eq("the driver answers with the config", reply._command, "CONFIG_DATA")
  T.eq("the property is set", Properties["Write Precision"], "s")
  T.eq("and the answer shows it", property("Write Precision").value, "s")
  write()
  local url
  for i = mark + 1, #posts do
    url = posts[i]:match("/api/v2/write%?.*") or url
  end
  T.truthy("its handler ran: the next write asks for seconds", url and url:find("precision=s", 1, true), url)
  set("Write Precision", "ms")
end

T.section("Changes the tab is turned away from")
do
  local reply = set("Write Precision", "minutes")
  T.eq("a value the list doesn't have", reply._command, "PROPERTY_ERROR")
  local refusal = JSON:decode(reply.payload or "{}")
  T.eq("names the property, in one JSON string", refusal.property, "Write Precision")
  T.truthy("and says why", (refusal.error or "") ~= "", refusal.error)
  T.eq("and the property keeps its value", Properties["Write Precision"], "ms")
  T.eq("a read-only property", set("Driver Status", "Fine")._command, "PROPERTY_ERROR")
  T.eq("a password", set("API Token", "x")._command, "PROPERTY_ERROR")
end

T.section("A change made in Composer reaches the tab")
do
  local mark = #pushes
  Properties["Log Mode"] = "Print"
  OnPropertyChanged("Log Mode")
  T.eq("the tab is sent the config", pushed("CONFIG_DATA", mark), 1)
  T.eq("with the new value", property("Log Mode").value, "Print")
  Properties["Log Mode"] = "Off"
  OnPropertyChanged("Log Mode")
end

T.section("A new server or database reaches the status card at once")
do
  local mark = #pushes
  set("Database", "garden")
  local status
  for i = mark + 1, #pushes do
    if pushes[i].command == "STATUS_DATA" then
      status = JSON:decode(pushes[i].data.status)
    end
  end
  T.truthy("the tab is sent the status", status ~= nil)
  T.eq("with the new database", status and status.database, "garden")
  set("Database", "home")
end

T.section("The status card")
do
  local status = JSON:decode(JSON:decode(UIR._GET_STATUS()).status)
  T.eq("Driver Status is sent exactly as Composer shows it", status.driverStatus, Properties["Driver Status"])
  T.eq("with the watchdog count", status.watchdogFires, 0)
  T.truthy("and the last write", (status.lastWrite or 0) > 0, status.lastWrite)
  local mark = #pushes
  watts = watts + 1
  OnWatchedVariableChanged(common.SENSOR, 1000, tostring(watts))
  cloud.poll(1)
  -- The interval tick and the write that follows it change the counts several times.
  ShimFireTimers()
  T.eq("counts that change wait for the push", pushed("STATUS_DATA", mark), 0)
  ShimFireTimers()
  T.eq("and go out once, a second later", pushed("STATUS_DATA", mark), 1)
end

T.section("The page")
do
  local fh = assert(io.open(ROOT .. "drivers/influxdb/www/html/index.html", "r"))
  local page = fh:read("*a")
  fh:close()
  T.truthy("the tab bar is hidden by default", page:find("%.nav {%s*display: none;") ~= nil)
  T.truthy(
    "and shown from two tabs",
    page:find("classList.toggle('multi', document.querySelectorAll('.nav-tab').length > 1)", 1, true) ~= nil
  )
  T.eq("no Settings tab", page:find('data-panel="settings"', 1, true), nil)
  T.truthy("a Properties card on the Status tab", page:find('id="props-card"', 1, true) ~= nil)
end

T.section("A Support Code changes Log Level's list")
do
  -- No secret in a test build, so the code's own change is told directly.
  local mark = #pushes
  require("lib.support-code"):_tell(true)
  T.eq("the tab is sent the config", pushed("CONFIG_DATA", mark), 1)
end

T.section("Nothing reaches the tab once the driver is destroyed")
do
  watts = watts + 1
  OnWatchedVariableChanged(common.SENSOR, 1000, tostring(watts))
  cloud.poll(1)
  ShimFireTimers()
  local mark = #pushes
  OnDriverDestroyed()
  ShimFireTimers()
  ShimFireTimers()
  T.eq("no status push, the pending one included", pushed("STATUS_DATA", mark), 0)
end

T.finish()
