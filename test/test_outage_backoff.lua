-- Retries through an InfluxDB outage, in the real driver on a clock of the test's own: every reading is written
-- again on its interval, and that mustn't retry a server that can't be reached once a tick.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_outage_backoff.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local now = 0
local answer = { code = 204 }
local writes = {}
function C4:urlPost(url, _, _, _, callback)
  if tostring(url):find("/api/v2/write", 1, true) then
    writes[#writes + 1] = now
  end
  if callback then
    callback(1, "", answer.code, {}, answer.err)
  end
  return 1
end

OnWatchedVariableChanged(common.SENSOR, 1000, "100")
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

local function since(t0)
  local n = 0
  for _, at in ipairs(writes) do
    if at > t0 then
      n = n + 1
    end
  end
  return n
end

-- Moves the reading's interval onto this clock: the timers it had came from the shim's.
UIRequest(
  "_UPDATE_MEAS_SETTINGS",
  { DATA = C4:Base64Encode(JSON:encode({ measurement = "power", interval = "1m", dedup = false, enabled = true })) }
)

T.section("An hour the server can't be reached")
do
  answer = { code = 0, err = "Connection refused" }
  local start = now
  runUntil(start + 3600)
  print(string.format("  write attempts in the hour: %d, in its last half: %d", since(start), since(start + 1800)))
  T.eq("Driver Status reads Disconnected", Properties["Driver Status"], "Disconnected")
  local first
  for i, at in ipairs(writes) do
    if at > start then
      first = i
      break
    end
  end
  local gaps = {}
  for i = first + 1, first + 6 do
    gaps[#gaps + 1] = writes[i] and writes[i] - writes[i - 1]
  end
  T.eq("each retry waits longer, up to 15 minutes", gaps, { 5, 15, 30, 60, 300, 900 })
  T.truthy("the last half hour tries at most twice", since(start + 1800) <= 2, since(start + 1800))
end

T.section("The server comes back")
do
  answer = { code = 204 }
  local back = now
  runUntil(back + 900)
  T.eq("its points go out by the next retry", Properties["Driver Status"], "Connected")
  T.eq("and nothing is left waiting", tonumber(Variables.INFLUX_POINTS_BUFFERED), 0)
  local delivered = now
  runUntil(delivered + 300)
  T.eq("a reading is written each minute again", since(delivered), 5)
end

T.finish()
