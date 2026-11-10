-- The Status tab's Properties card and status card, in the real driver: the properties the tab is sent, a change made
-- from it, the changes it is turned away from, a change made in Composer reaching it, the status it shows, and the
-- page wired to lib/ui for its tab bar and Properties card.
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
function C4:SendDataToUI(command, params)
  pushes[#pushes + 1] = { command = command, json = params.json, data = JSON:decode(params.json) }
end

local function data(t)
  return { DATA = C4:Base64Encode(JSON:encode(t)) }
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

--- The last push of a command since a mark.
local function last(command, since)
  local found
  for i = (since or 0) + 1, #pushes do
    if pushes[i].command == command then
      found = pushes[i]
    end
  end
  return found
end

--- Sets a property from the tab.
--- @return table|nil refusal What the driver pushed back, or nil once it is set.
local function set(name, value)
  local mark = #pushes
  UIR._SET_PROPERTY(data({ property = name, value = value }))
  local refusal = last("PROPERTY_ERROR", mark)
  return refusal and refusal.data
end

--- What the properties card is sent when it asks.
local function shown()
  local mark = #pushes
  UIR._GET_PROPERTIES()
  return last("PROPERTIES", mark).data.properties
end

local function property(name)
  for _, p in ipairs(shown()) do
    if p.name == name then
      return p
    end
  end
end

--- The raw message a command pushes when asked.
local function asked(command, answer)
  local mark = #pushes
  UIR[command]()
  return last(answer, mark).json
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
  T.excludes("and its value never reaches the tab's properties", asked("_GET_PROPERTIES", "PROPERTIES"), "throwaway")
  T.excludes("or its config", asked("_GET_CONFIG", "CONFIG_DATA"), "throwaway")
  T.excludes("or its status", asked("_GET_STATUS", "STATUS_DATA"), "throwaway")
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
  local SHOWN = { name = true, default = true, item = true, description = true, display_name = true }
  SHOWN.true_text, SHOWN.false_text = true, true
  for tag, text in xml:gmatch("<([%w_]+)>([^<]*&amp;[^<]*)</[%w_]+>") do
    if SHOWN[tag] then
      shown[#shown + 1] = text
    end
  end
  T.eq("no text Composer shows holds an &", shown, {})
end

T.section("A change from the tab")
do
  local mark = #posts
  T.eq("is taken", set("Write Precision", "s"), nil)
  T.eq("the property is set", Properties["Write Precision"], "s")
  T.eq("and the card's next ask shows it", property("Write Precision").value, "s")
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
  local refusal = set("Write Precision", "minutes") or {}
  T.eq("a value the list doesn't have names the property", refusal.property, "Write Precision")
  T.truthy("and says why", (refusal.error or "") ~= "", refusal.error)
  T.eq("and the property keeps its value", Properties["Write Precision"], "ms")
  T.neq("a read-only property", set("Driver Status", "Fine"), nil)
  T.neq("a password", set("API Token", "x"), nil)
end

T.section("A change made in Composer reaches the tab")
do
  local mark = #pushes
  Properties["Log Mode"] = "Print"
  OnPropertyChanged("Log Mode")
  T.eq("the tab is sent the status", pushed("STATUS_DATA", mark), 1)
  T.eq("and the card's next ask shows the new value", property("Log Mode").value, "Print")
  Properties["Log Mode"] = "Off"
  OnPropertyChanged("Log Mode")
end

T.section("A new server or database reaches the status card at once")
do
  local mark = #pushes
  set("Database", "garden")
  local status = (last("STATUS_DATA", mark) or {}).data
  T.truthy("the tab is sent the status", status ~= nil)
  T.eq("with the new database", status and status.database, "garden")
  set("Database", "home")
end

T.section("The status card")
do
  local status = JSON:decode(asked("_GET_STATUS", "STATUS_DATA"))
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
  -- The tab bar and the Properties card are lib/ui's, which its own tests cover; this checks the page is wired to it.
  T.truthy(
    "the page embeds the kit's stylesheet",
    page:find("<!-- #embed ../../../../src/lib/ui/style.css -->", 1, true) ~= nil
  )
  T.truthy("and its script", page:find("<!-- #embed ../../../../src/lib/ui/page.js -->", 1, true) ~= nil)
  T.truthy("which runs the tab bar", page:find("Kit.initTabs(onTabSwitch, { hash: true })", 1, true) ~= nil)
  T.truthy("and the Properties card", page:find("initPropertiesCard(document.getElementById('props'))", 1, true) ~= nil)
  T.eq("no Settings tab", page:find('data-panel="settings"', 1, true), nil)
  T.truthy("a Properties card on the Status tab", page:find('id="props-card"', 1, true) ~= nil)
end

T.section("A Support Code changes Log Level's list")
do
  -- No secret in a test build, so the code's own change is made directly.
  local code = require("lib.support-code")
  code:_allowLevels(false)
  local closed = #property("Log Level").choices
  code:_allowLevels(true)
  T.truthy("the card's next ask offers Trace and Ultra", #property("Log Level").choices > closed)
  code:_allowLevels(false)
  T.eq("and the one after the hour's end stops at Debug", #property("Log Level").choices, closed)
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
