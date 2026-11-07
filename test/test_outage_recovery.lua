-- Recovery after an InfluxDB outage, in the real driver on a clock of the test's own: once anything gets through,
-- every measurement waiting out a retry sends at once, except one the server itself asked to wait.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_outage_recovery.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local now = 0
local DOWN = { code = 0, err = "Connection refused" }
local UP = { code = 204 }
--- The server's answer to each measurement's writes, and to a connection test.
local answers = { power = UP, energy = UP, test = UP }
local writes = {}
function C4:urlPost(url, data, _, _, callback)
  local measurement = tostring(data):match("^([^, ]+)")
  local answer = answers[measurement] or UP
  if tostring(url):find("/api/v2/write", 1, true) then
    writes[#writes + 1] = { at = now, measurement = measurement, ok = answer.code == 204 }
  end
  if callback then
    callback(1, "", answer.code, answer.headers or {}, answer.err)
  end
  return 1
end
urlDo = function(_, _, _, _, callback)
  if callback then
    callback(answers.test.err, answers.test.code, {}, "", nil)
  end
end

local function data(t)
  return { DATA = C4:Base64Encode(JSON:encode(t)) }
end

UIRequest("_ADD_MEASUREMENT", data({ name = "energy" }))
UIRequest("_ADD_FIELD_DEF", data({ measurement = "energy", name = "kwh" }))
UIRequest("_ADD_READING", data({ measurement = "energy", label = "main" }))
UIRequest("_SAVE_MAPPING", {
  DATA = data({
    measurement = "energy",
    reading = "main",
    name = "kwh",
    mapping = { source = "variable", varId = common.SENSOR .. ":1001" },
  }).DATA,
})
OnWatchedVariableChanged(common.SENSOR, 1000, "100")
OnWatchedVariableChanged(common.SENSOR, 1001, "7")
cloud.poll(1)
ShimFireTimers()
T.eq("the driver starts Connected", Properties["Driver Status"], "Connected")

-- From here every timer runs on `now`, which only runUntil moves.
local timers = {}
function C4:SetTimer(ms, fn, repeating)
  local t = { at = now + ms / 1000, every = repeating and ms / 1000 or nil, fn = fn }
  t.handle = newproxy(true)
  getmetatable(t.handle).__index = {
    Cancel = function()
      t.cancelled = true
    end,
  }
  timers[#timers + 1] = t
  return t.handle
end

--- Runs every timer that comes due up to `stop`, in order.
local function runUntil(stop)
  while true do
    local nextTimer
    for _, t in ipairs(timers) do
      if not t.cancelled and t.at <= stop and (not nextTimer or t.at < nextTimer.at) then
        nextTimer = t
      end
    end
    if not nextTimer then
      break
    end
    now = nextTimer.at
    if nextTimer.every then
      nextTimer.at = now + nextTimer.every
    else
      nextTimer.cancelled = true
    end
    nextTimer.fn(nextTimer.handle, 0)
  end
  now = stop
end

--- When each measurement's first write after `t0` got through.
local function firstOk(t0)
  local at = {}
  for _, w in ipairs(writes) do
    if w.at > t0 and w.ok and not at[w.measurement] then
      at[w.measurement] = w.at
    end
  end
  return at
end

-- Two schedules, so the two measurements fail, and wait out their retries, at different times.
UIRequest(
  "_UPDATE_MEAS_SETTINGS",
  { DATA = C4:Base64Encode(JSON:encode({ measurement = "power", interval = "1m", dedup = false, enabled = true })) }
)
UIRequest(
  "_UPDATE_MEAS_SETTINGS",
  { DATA = C4:Base64Encode(JSON:encode({ measurement = "energy", interval = "5m", dedup = false, enabled = true })) }
)

--- An hour in which no write gets through, which leaves both measurements on 15-minute retries.
local function outage()
  answers = { power = DOWN, energy = DOWN, test = DOWN }
  runUntil(now + 3600)
  T.eq("Driver Status reads Disconnected", Properties["Driver Status"], "Disconnected")
end

T.section("The server comes back, and one measurement's retry gets through")
do
  outage()
  local back = now
  answers = { power = UP, energy = UP, test = UP }
  runUntil(back + 900)
  local at = firstOk(back)
  print(string.format("  first write back: power at +%ds, energy at +%ds", at.power - back, at.energy - back))
  T.eq("the other measurement sends at the same moment", at.energy, at.power)
  T.eq("nothing is left waiting", tonumber(Variables.INFLUX_POINTS_BUFFERED), 0)
end

T.section("A connection test gets through first")
do
  outage()
  answers = { power = UP, energy = UP, test = UP }
  local back = now
  OnPropertyChanged("Database")
  T.eq("Driver Status reads Connected", Properties["Driver Status"], "Connected")
  local at = firstOk(back - 1)
  T.eq("power sends at once", at.power, back)
  T.eq("energy sends at once", at.energy, back)
  T.eq("nothing is left waiting", tonumber(Variables.INFLUX_POINTS_BUFFERED), 0)
end

T.section("A measurement the server asked to wait")
do
  runUntil(now + 600)
  answers = {
    power = DOWN,
    energy = { code = 429, headers = { ["Retry-After"] = "600" } },
    test = DOWN,
  }
  local start = now
  runUntil(start + 300)
  local asked
  for _, w in ipairs(writes) do
    if w.at > start and w.measurement == "energy" and not asked then
      asked = w.at
    end
  end
  T.truthy("energy was asked to wait", asked)
  answers = { power = UP, energy = UP, test = UP }
  local back = now
  runUntil(back + 900)
  local at = firstOk(back)
  print(string.format("  first write back: power at +%ds, energy at +%ds", at.power - back, at.energy - back))
  T.truthy("power gets through first", at.power < at.energy, at.power - back)
  T.truthy("energy keeps the server's 10 minutes", at.energy - asked >= 600, at.energy - asked)
end

T.section("A measurement the server keeps refusing while the other gets through")
do
  answers = { power = UP, energy = { code = 503 }, test = UP }
  local start = now
  runUntil(start + 3600)
  local tries = 0
  for _, w in ipairs(writes) do
    if w.at > start + 1800 and w.measurement == "energy" then
      tries = tries + 1
    end
  end
  print(string.format("  energy's tries in the hour's last half: %d", tries))
  T.truthy("its retries still wait longer each time", tries <= 2, tries)
end

T.finish()
