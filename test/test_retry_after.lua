-- A 429 whose Retry-After asks for no wait, in the real driver on a clock of the test's own whose timers,
-- like Director's, refuse a delay under 1 ms: the batch must still go out again.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_retry_after.lua

require("c4_shim")
local T = require("testlib")
local cloud = require("dc_cloud")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local now = 0
local queued = {}
local writes = {}
function C4:urlPost(url, _, _, _, callback)
  local answer = { code = 204 }
  if tostring(url):find("/api/v2/write", 1, true) then
    answer = table.remove(queued, 1) or answer
    writes[#writes + 1] = { at = now, code = answer.code }
  end
  if callback then
    callback(1, "", answer.code, answer.headers or {}, answer.err)
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
  if type(ms) ~= "number" then
    error("milliseconds should be a number")
  end
  if ms < 1 then
    error("Invalid argument value")
  end
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

-- Moves the reading's interval onto this clock: the timers it had came from the shim's.
UIRequest(
  "_UPDATE_MEAS_SETTINGS",
  { DATA = C4:Base64Encode(JSON:encode({ measurement = "power", interval = "1m", dedup = false, enabled = true })) }
)

T.section("A 429 that asks for no wait")
do
  queued = { { code = 429, headers = { ["Retry-After"] = "0" } } }
  runUntil(now + 120)
  local limited
  for i, w in ipairs(writes) do
    if w.code == 429 then
      limited = i
    end
  end
  T.truthy("the server answered 429", limited)
  local retry = limited and writes[limited + 1]
  T.eq("the batch goes out again a second later", retry and retry.at - writes[limited].at, 1)
  T.eq("and gets through", retry and retry.code, 204)
  T.eq("nothing is left waiting", tonumber(Variables.INFLUX_POINTS_BUFFERED), 0)
  T.eq("Driver Status reads Connected", Properties["Driver Status"], "Connected")
end

T.finish()
