--- Tests for lib/influx_writer.lua
--- Run from repo root: lua test/test_influx_writer.lua

local script_dir = debug.getinfo(1, "S").source:match("^@(.+)/[^/]+$") or "."
package.path = script_dir .. "/../src/?.lua;" .. script_dir .. "/../src/?/init.lua;" .. package.path

dofile(script_dir .. "/c4_shim.lua")

-- The real helpers, so the tests exercise what the driver actually runs:
-- IsEmpty/tointeger/toboolean/TableDeepCopy from utils, Serialize/Deserialize/
-- Select from the common lib, SetTimer/CancelTimer from the common timer,
-- UpdateProperty from the common handlers. Flush timers are never advanced --
-- the shim's C4:SetTimer only fires under C4:ProcessTimers -- so tests drive
-- flushes directly.
require("lib.utils")
require("drivers-common-public.global.lib")
require("drivers-common-public.global.timer")
require("drivers-common-public.global.handlers")

-- Control4 variable API, which neither the shim nor the common libs provide
Variables = {}
function C4:AddVariable(name, value)
  Variables[name] = value
end
function C4:SetVariable(name, value)
  Variables[name] = value
end
function C4:DeleteVariable(name)
  Variables[name] = nil
end

local passed = 0
local failed = 0

local function test(name, fn)
  local ok, err = pcall(fn)
  if ok then
    print("  PASS: " .. name)
    passed = passed + 1
  else
    print("  FAIL: " .. name .. "\n    " .. tostring(err))
    failed = failed + 1
  end
end

local function assert_eq(a, b, msg)
  if a ~= b then
    error(string.format("%s: expected %s, got %s", msg or "assertion failed", tostring(b), tostring(a)))
  end
end

local function assert_true(v, msg)
  if not v then
    error(msg or "expected true")
  end
end

print("\n=== InfluxWriter tests ===\n")

local InfluxWriter = require("lib.influx_writer")

--- Capture posted batches instead of hitting the network. Returns the log of
--- batches and a function to settle the pending request.
local function captureWrites()
  local posts = {}
  local pending = {}
  InfluxWriter.postBatch = function(url, token, lines)
    local d = { _ok = nil, _err = nil }
    function d:next(onOk, onErr)
      self._ok = self._ok or onOk
      self._err = self._err or onErr
      return self
    end
    posts[#posts + 1] = lines
    pending[#pending + 1] = d
    return d
  end
  return posts, pending
end

local function settle(pending, index, ok)
  local d = pending[index]
  if ok ~= false then
    if d._ok then
      d._ok({ count = 0 })
    end
  elseif d._err then
    d._err({ retriable = true, errMsg = "boom" })
  end
end

local function newWriter()
  return InfluxWriter:new({
    getConfig = function()
      return { url = "http://influx.test", database = "db", precision = "ms", token = "t" }
    end,
  })
end

local function field(v)
  return { value = v, type = "integer" }
end

-- ---------------------------------------------------------------

test("inferValueType and formatFieldValue agree on what a value becomes", function()
  -- The preview and the write path share these two, so the pairing is the
  -- invariant, not either function alone.
  local cases = {
    { "0", nil, "0i" },
    { "0", "string", '"0"' },
    { "0", "float", "0.0" },
    { "0", "boolean", "false" },
    { "57", "float", "57.0" },
    { "45.7", nil, "45.7" },
    { "45.7", "integer", "45i" },
    { "-4.3", "integer", "-5i" },
    { "true", nil, "true" },
    { "Master Bathroom", nil, '"Master Bathroom"' },
  }
  for _, c in ipairs(cases) do
    local raw, pin, want = c[1], c[2], c[3]
    local vt = pin or InfluxWriter.inferValueType(raw)
    local got = InfluxWriter.formatFieldValue(raw, vt)
    assert_eq(got, want, string.format("%q as %s", raw, tostring(pin or "inferred")))
  end
end)

test("a value that cannot coerce to the pinned type reports an error", function()
  local got, err = InfluxWriter.formatFieldValue("Idle", "integer")
  assert_true(got == nil, "no formatted value")
  assert_true(err ~= nil and err:find("Idle") ~= nil, "error names the value")
end)

test("buildLine includes the supplied timestamp", function()
  local line = InfluxWriter.buildLine("m", { room = "Den" }, { connected = field(1) }, 1786302810144)
  assert_eq(line, "m,room=Den connected=1i 1786302810144", "line protocol")
end)

test("buildLine omits the timestamp when none is given", function()
  local line = InfluxWriter.buildLine("m", {}, { connected = field(1) })
  assert_eq(line, "m connected=1i", "line protocol")
end)

test("readings of one measurement share a buffer and post once", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue(
    "connectivity",
    { d = "a" },
    { connected = field(1) },
    { interval = 60, dedup = false, dedupKey = "c::a" },
    1
  )
  w:enqueue(
    "connectivity",
    { d = "b" },
    { connected = field(1) },
    { interval = 60, dedup = false, dedupKey = "c::b" },
    1
  )
  w:forceFlushAll()

  assert_eq(#posts, 1, "one HTTP request")
  assert_eq(#posts[1], 2, "both points in the batch")
end)

test("differing intervals keep separate buffers", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("m", { d = "a" }, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:enqueue("m", { d = "b" }, { v = field(1) }, { interval = 10, dedup = false, dedupKey = "m::b" }, 1)
  w:forceFlushAll()

  assert_eq(#posts, 2, "one request per interval")
end)

test("dedup is scoped per reading, not per shared buffer", function()
  local posts = captureWrites()
  local w = newWriter()
  local a = { interval = 60, dedup = true, dedupKey = "m::a" }
  local b = { interval = 60, dedup = true, dedupKey = "m::b" }

  w:enqueue("m", { d = "a" }, { v = field(1) }, a, 1)
  w:enqueue("m", { d = "b" }, { v = field(1) }, b, 1)
  -- 'a' repeats its value and is skipped; 'b' changes and is kept. Sharing one
  -- lastValues table would have let b's write mask a's repeat, or vice versa.
  w:enqueue("m", { d = "a" }, { v = field(1) }, a, 2)
  w:enqueue("m", { d = "b" }, { v = field(2) }, b, 2)
  w:forceFlushAll()

  assert_eq(#posts[1], 3, "two initial points plus b's change")
end)

test("a flush does not start while one is in flight", function()
  local posts, pending = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  assert_eq(#posts, 1, "first request issued")

  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 2)
  w:forceFlushAll()
  assert_eq(#posts, 1, "second flush suppressed while in flight")

  settle(pending, 1, true)
  w:forceFlushAll()
  assert_eq(#posts, 2, "flush resumes once the request settles")
end)

test("a failed write clears in-flight so the retry can run", function()
  local posts, pending = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  settle(pending, 1, false)

  w:forceFlushAll()
  assert_eq(#posts, 2, "requeued batch is retried")
end)

test("removeMeasurement drops every interval's buffer", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:enqueue("m", {}, { v = field(1) }, { interval = 10, dedup = false, dedupKey = "m::b" }, 1)
  w:removeMeasurement("m")
  w:forceFlushAll()

  assert_eq(#posts, 0, "nothing left to flush")
end)

test("removeReading forgets only that reading's dedup history", function()
  local posts = captureWrites()
  local w = newWriter()
  local a = { interval = 60, dedup = true, dedupKey = "m::a" }
  local b = { interval = 60, dedup = true, dedupKey = "m::b" }

  w:enqueue("m", { d = "a" }, { v = field(1) }, a, 1)
  w:enqueue("m", { d = "b" }, { v = field(1) }, b, 1)
  w:removeReading("m", "m::a")
  -- 'a' re-enqueues its old value because its history was dropped; 'b' still dedups.
  w:enqueue("m", { d = "a" }, { v = field(1) }, a, 2)
  w:enqueue("m", { d = "b" }, { v = field(1) }, b, 2)
  w:forceFlushAll()

  assert_eq(#posts[1], 3, "a re-enqueued, b deduped")
end)

test("a measurement whose name contains @ does not take siblings with it", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("power", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "power::a" }, 1)
  w:enqueue("power@rack", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "power@rack::a" }, 1)
  w:removeMeasurement("power")
  w:forceFlushAll()

  assert_eq(#posts, 1, "only 'power' removed")
  assert_true(posts[1][1]:match("^power@rack") ~= nil, "surviving measurement is 'power@rack'")
end)

test("buffered points survive a removed measurement's sibling", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("keep", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "keep::a" }, 1)
  w:enqueue("drop", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "drop::a" }, 1)
  w:removeMeasurement("drop")
  w:forceFlushAll()

  assert_eq(#posts, 1, "only the surviving measurement flushes")
  assert_true(posts[1][1]:match("^keep") ~= nil, "surviving measurement is 'keep'")
end)

test("retriable failures walk the backoff ladder and reset on success", function()
  local _, pending = captureWrites()
  local w = newWriter()
  local delays = {}
  local origSetTimer = SetTimer
  SetTimer = function(name, ms, fn)
    delays[#delays + 1] = ms / 1000
    return origSetTimer(name, ms, fn)
  end

  local seq = 0
  local function failOnce()
    seq = seq + 1
    w:enqueue("m", {}, { v = field(seq) }, { interval = 60, dedup = false, dedupKey = "m::a" }, seq)
    w:forceFlushAll()
    settle(pending, #pending, false)
  end

  failOnce()
  failOnce()
  failOnce()
  SetTimer = origSetTimer

  -- constants.RETRY_INTERVALS = { 5, 15, 30, 60, 300, 900 }; every failure used
  -- to re-arm at 5.
  assert_eq(delays[1], 5, "first retry at 5s")
  assert_eq(delays[2], 15, "second retry climbs to 15s")
  assert_eq(delays[3], 30, "third retry climbs to 30s")

  -- A success resets the ladder, so the next failure starts at the bottom again.
  SetTimer = function(name, ms, fn)
    delays[#delays + 1] = ms / 1000
    return origSetTimer(name, ms, fn)
  end
  w:enqueue("m", {}, { v = field(99) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 99)
  w:forceFlushAll()
  settle(pending, #pending, true)
  w:enqueue("m", {}, { v = field(100) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 100)
  w:forceFlushAll()
  settle(pending, #pending, false)
  SetTimer = origSetTimer
  assert_eq(delays[#delays], 5, "ladder resets to 5s after a successful write")
end)

test("watchdog clears a flush stuck in flight after a lost callback", function()
  local posts = captureWrites()
  local w = newWriter()
  local now = 1000
  local origTime = os.time
  os.time = function()
    return now
  end

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  assert_eq(#posts, 1, "first request issued, inFlight set")

  -- The callback never fires. Within the watchdog window a re-flush is suppressed.
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)
  now = 1000 + 100
  w:forceFlushAll()
  assert_eq(#posts, 1, "still suppressed inside the watchdog window")

  -- threshold = max(300, 60*5) = 300s. Past it, the watchdog clears inFlight.
  now = 1000 + 301
  w:forceFlushAll()
  os.time = origTime
  assert_eq(#posts, 2, "watchdog recovered the wedged buffer")
  assert_eq(w._metrics.watchdogFires, 1, "watchdog fire recorded")
  -- The publish after the fire count bump is load-bearing, not a duplicate of the
  -- one _restoreBatch does: only it carries the incremented count to the variable.
  assert_eq(tonumber(Variables["INFLUX_WATCHDOG_FIRES"]), 1, "watchdog-fire variable published, not left stale")
end)

test("shutdown stops every flush path from re-arming a timer", function()
  local _, pending = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll() -- posts[1], inFlight true, nothing settled

  -- A second batch is waiting, so the success handler would re-arm on completion.
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)

  w:shutdown() -- forceFlushAll here hits the in-flight branch, which would re-arm
  settle(pending, 1, true) -- success handler sees buffer > 0 and would re-arm

  local anyArmed = false
  for _, state in pairs(w._measurements) do
    if state.timerName then
      anyArmed = true
    end
  end
  assert_true(not anyArmed, "no flush timer armed after shutdown")
end)

test("watchdog restores the stuck batch instead of dropping it", function()
  local posts = captureWrites()
  local w = newWriter()
  local now = 1000
  local origTime = os.time
  os.time = function()
    return now
  end

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  assert_eq(#posts[1], 1, "first request carries the point")

  -- The callback is lost. A new point keeps the buffer non-empty so the flush
  -- reaches the watchdog rather than returning early.
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)
  now = 1000 + 301
  w:forceFlushAll()
  os.time = origTime

  assert_eq(#posts, 2, "reissued after the watchdog fire")
  assert_eq(#posts[2], 2, "the stuck point was restored to the batch, not lost")
  assert_eq(w._metrics.pointsDropped, 0, "the recovered point is not counted as dropped")
end)

test("a superseded request's late success does not reopen concurrency", function()
  local posts, pending = captureWrites()
  local w = newWriter()
  local now = 1000
  local origTime = os.time
  os.time = function()
    return now
  end

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll() -- request A
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)
  now = 1000 + 301
  w:forceFlushAll() -- watchdog fires, reissues as request B (inFlight stays true)
  assert_eq(#posts, 2, "reissued as B")

  -- A finally lands. Its stale success handler must not clear B's inFlight.
  settle(pending, 1, true)
  now = 1000
  w:enqueue("m", {}, { v = field(3) }, { interval = 60, dedup = false, dedupKey = "m::c" }, 3)
  w:forceFlushAll()
  os.time = origTime
  assert_eq(#posts, 2, "B is still in flight, so no third concurrent request")
end)

test("a superseded request's late rejection does not restore its batch or retry", function()
  local posts, pending = captureWrites()
  local w = newWriter()
  local now = 1000
  local origTime = os.time
  os.time = function()
    return now
  end

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll() -- request A
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)
  now = 1000 + 301
  w:forceFlushAll() -- watchdog fires, reissues as request B, bumps the generation
  os.time = origTime
  assert_eq(#posts, 2, "reissued as B")

  local m60 = w._measurements["m@60"]
  local retryBefore = m60.retryIndex
  settle(pending, 1, false) -- A's late rejection, now stale
  assert_eq(m60.retryIndex, retryBefore, "stale rejection did not advance the backoff ladder")
  assert_true(m60.timerName == nil, "stale rejection did not arm a retry timer")
end)

test("a restored batch is kept even when it overshoots the cap", function()
  local _, pending = captureWrites()
  local w = newWriter()
  local function enq(i)
    w:enqueue("m", {}, { v = field(i) }, { interval = 60, dedup = false, maxBuffer = 3, dedupKey = "m::" .. i }, i)
  end

  enq(1)
  enq(2)
  enq(3)
  w:forceFlushAll() -- batch [1,2,3] out, buffer empty, inFlight
  enq(4)
  enq(5)
  enq(6) -- buffer fills to the cap of 3: [4,5,6]

  settle(pending, 1, false) -- reject: restore [1,2,3] ahead of [4,5,6]

  -- The restore keeps every point rather than dropping the batch it recovered,
  -- overshooting the cap for one flush cycle. Trimming here would lose exactly
  -- the retried points on a transient failure the next flush would have cleared.
  local m60 = w._measurements["m@60"]
  assert_eq(#m60.buffer, 6, "recovered and buffered points all kept, cap overshot transiently")
  assert_eq(w._metrics.pointsDropped, 0, "the restore drops nothing")
  -- Order matters: the recovered batch goes to the front, ahead of what was
  -- enqueued while it was out, so a later cap eviction takes the oldest first.
  assert_true(m60.buffer[1]:find("v=1i", 1, true) ~= nil, "recovered batch sits at the front, not appended")
  assert_true(m60.buffer[6]:find("v=6i", 1, true) ~= nil, "points enqueued during the flush stay at the back")
  -- The published variable must track the restore, not read a stale zero while
  -- points pile up unsent through an outage.
  assert_eq(tonumber(Variables["INFLUX_POINTS_BUFFERED"]), 6, "buffered variable reflects the restore")
end)

print(string.format("\n%d passed, %d failed\n", passed, failed))
if failed > 0 then
  os.exit(1)
end
