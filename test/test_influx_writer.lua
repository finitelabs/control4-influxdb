-- Tests for lib/influx_writer.lua
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_influx_writer.lua

local T = require("testlib")

-- The shim owns the C4 environment, including the variable API it validates the
-- way Director does. The real helpers come next so the tests exercise what the
-- driver actually runs: IsEmpty/tointeger/toboolean/TableDeepCopy from utils,
-- Serialize/Deserialize/Select from the common lib, SetTimer/CancelTimer from
-- the common timer, UpdateProperty from the common handlers. Flush timers are
-- never advanced (the shim's C4:SetTimer only fires under C4:ProcessTimers), so
-- tests drive flushes directly.
require("c4_shim")
require("lib.utils")
require("drivers-common-public.global.lib")
require("drivers-common-public.global.timer")
require("drivers-common-public.global.handlers")

--- One scenario. testlib assertions do not raise, so an error escaping fn is an
--- unexpected crash rather than a failed expectation, and has to be recorded or
--- the scenario passes by vanishing.
local function case(name, fn)
  local ok, err = pcall(fn)
  if not ok then
    T.check(name, false, err)
  end
end

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

T.section("Value typing")

case("inferValueType and formatFieldValue agree on what a value becomes", function()
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
    T.eq(string.format("%q as %s", raw, tostring(pin or "inferred")), InfluxWriter.formatFieldValue(raw, vt), want)
  end
end)

case("a value that cannot coerce to the pinned type reports an error", function()
  local got, err = InfluxWriter.formatFieldValue("Idle", "integer")
  T.eq("no formatted value is returned", got, nil)
  T.truthy("the error names the offending value", err ~= nil and err:find("Idle") ~= nil, err)
end)

T.section("Line protocol")

case("buildLine includes the supplied timestamp", function()
  T.eq(
    "measurement, tag, field and timestamp",
    InfluxWriter.buildLine("m", { room = "Den" }, { connected = field(1) }, 1786302810144),
    "m,room=Den connected=1i 1786302810144"
  )
end)

case("buildLine omits the timestamp when none is given", function()
  T.eq("no trailing timestamp", InfluxWriter.buildLine("m", {}, { connected = field(1) }), "m connected=1i")
end)

T.section("Buffering and dedup")

case("readings of one measurement share a buffer and post once", function()
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

  T.eq("one HTTP request", #posts, 1)
  T.eq("both points in the batch", #posts[1], 2)
end)

case("differing intervals keep separate buffers", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("m", { d = "a" }, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:enqueue("m", { d = "b" }, { v = field(1) }, { interval = 10, dedup = false, dedupKey = "m::b" }, 1)
  w:forceFlushAll()

  T.eq("one request per interval", #posts, 2)
end)

case("dedup is scoped per reading, not per shared buffer", function()
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

  T.eq("two initial points plus b's change", #posts[1], 3)
end)

T.section("Flush concurrency")

case("a flush does not start while one is in flight", function()
  local posts, pending = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  T.eq("first request issued", #posts, 1)

  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 2)
  w:forceFlushAll()
  T.eq("second flush suppressed while in flight", #posts, 1)

  settle(pending, 1, true)
  w:forceFlushAll()
  T.eq("flush resumes once the request settles", #posts, 2)
end)

case("a failed write clears in-flight so the retry can run", function()
  local posts, pending = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  settle(pending, 1, false)

  w:forceFlushAll()
  T.eq("requeued batch is retried", #posts, 2)
end)

T.section("Removal")

case("removeMeasurement drops every interval's buffer", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:enqueue("m", {}, { v = field(1) }, { interval = 10, dedup = false, dedupKey = "m::b" }, 1)
  w:removeMeasurement("m")
  w:forceFlushAll()

  T.eq("nothing left to flush", #posts, 0)
end)

case("removeReading forgets only that reading's dedup history", function()
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

  T.eq("a re-enqueued, b deduped", #posts[1], 3)
end)

case("a measurement whose name contains @ does not take siblings with it", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("power", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "power::a" }, 1)
  w:enqueue("power@rack", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "power@rack::a" }, 1)
  w:removeMeasurement("power")
  w:forceFlushAll()

  T.eq("only 'power' removed", #posts, 1)
  T.truthy("surviving measurement is 'power@rack'", posts[1][1]:match("^power@rack") ~= nil, posts[1][1])
end)

case("buffered points survive a removed measurement's sibling", function()
  local posts = captureWrites()
  local w = newWriter()

  w:enqueue("keep", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "keep::a" }, 1)
  w:enqueue("drop", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "drop::a" }, 1)
  w:removeMeasurement("drop")
  w:forceFlushAll()

  T.eq("only the surviving measurement flushes", #posts, 1)
  T.truthy("surviving measurement is 'keep'", posts[1][1]:match("^keep") ~= nil, posts[1][1])
end)

T.section("Retry backoff")

case("retriable failures walk the backoff ladder and reset on success", function()
  local _, pending = captureWrites()
  local w = newWriter()
  local delays = {}
  local origSetTimer = SetTimer
  SetTimer = function(name, ms, fn) -- luacheck: ignore
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
  SetTimer = origSetTimer -- luacheck: ignore

  -- constants.RETRY_INTERVALS = { 5, 15, 30, 60, 300, 900 }; every failure used
  -- to re-arm at 5.
  T.eq("first retry at 5s", delays[1], 5)
  T.eq("second retry climbs to 15s", delays[2], 15)
  T.eq("third retry climbs to 30s", delays[3], 30)

  -- A success resets the ladder, so the next failure starts at the bottom again.
  SetTimer = function(name, ms, fn) -- luacheck: ignore
    delays[#delays + 1] = ms / 1000
    return origSetTimer(name, ms, fn)
  end
  w:enqueue("m", {}, { v = field(99) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 99)
  w:forceFlushAll()
  settle(pending, #pending, true)
  w:enqueue("m", {}, { v = field(100) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 100)
  w:forceFlushAll()
  settle(pending, #pending, false)
  SetTimer = origSetTimer -- luacheck: ignore

  T.eq("ladder resets to 5s after a successful write", delays[#delays], 5)
end)

T.section("In-flight watchdog")

case("watchdog clears a flush stuck in flight after a lost callback", function()
  local posts = captureWrites()
  local w = newWriter()
  local now = 1000
  local origTime = os.time
  os.time = function()
    return now
  end

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  T.eq("first request issued, inFlight set", #posts, 1)

  -- The callback never fires. Within the watchdog window a re-flush is suppressed.
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)
  now = 1000 + 100
  w:forceFlushAll()
  T.eq("still suppressed inside the watchdog window", #posts, 1)

  -- threshold = max(300, 60*5) = 300s. Past it, the watchdog clears inFlight.
  now = 1000 + 301
  w:forceFlushAll()
  os.time = origTime

  T.eq("watchdog recovered the wedged buffer", #posts, 2)
  T.eq("watchdog fire recorded", w._metrics.watchdogFires, 1)
  -- The publish after the fire count bump is load-bearing, not a duplicate of the
  -- one _restoreBatch does: only it carries the incremented count to the variable.
  T.eq("watchdog-fire variable published, not left stale", tonumber(Variables["INFLUX_WATCHDOG_FIRES"]), 1)
end)

case("watchdog restores the stuck batch instead of dropping it", function()
  local posts = captureWrites()
  local w = newWriter()
  local now = 1000
  local origTime = os.time
  os.time = function()
    return now
  end

  w:enqueue("m", {}, { v = field(1) }, { interval = 60, dedup = false, dedupKey = "m::a" }, 1)
  w:forceFlushAll()
  T.eq("first request carries the point", #posts[1], 1)

  -- The callback is lost. A new point keeps the buffer non-empty so the flush
  -- reaches the watchdog rather than returning early.
  w:enqueue("m", {}, { v = field(2) }, { interval = 60, dedup = false, dedupKey = "m::b" }, 2)
  now = 1000 + 301
  w:forceFlushAll()
  os.time = origTime

  T.eq("reissued after the watchdog fire", #posts, 2)
  T.eq("the stuck point was restored to the batch, not lost", #posts[2], 2)
  T.eq("the recovered point is not counted as dropped", w._metrics.pointsDropped, 0)
end)

T.section("Superseded requests")

case("a superseded request's late success does not reopen concurrency", function()
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
  T.eq("reissued as B", #posts, 2)

  -- A finally lands. Its stale success handler must not clear B's inFlight.
  settle(pending, 1, true)
  now = 1000
  w:enqueue("m", {}, { v = field(3) }, { interval = 60, dedup = false, dedupKey = "m::c" }, 3)
  w:forceFlushAll()
  os.time = origTime

  T.eq("B is still in flight, so no third concurrent request", #posts, 2)
end)

case("a superseded request's late rejection does not restore its batch or retry", function()
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
  T.eq("reissued as B", #posts, 2)

  local m60 = w._measurements["m@60"]
  local retryBefore = m60.retryIndex
  settle(pending, 1, false) -- A's late rejection, now stale
  T.eq("stale rejection did not advance the backoff ladder", m60.retryIndex, retryBefore)
  T.eq("stale rejection did not arm a retry timer", m60.timerName, nil)
end)

T.section("Shutdown")

case("shutdown stops every flush path from re-arming a timer", function()
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
  T.falsy("no flush timer armed after shutdown", anyArmed)
end)

T.section("Buffer cap")

case("a restored batch is kept even when it overshoots the cap", function()
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
  T.eq("recovered and buffered points all kept, cap overshot transiently", #m60.buffer, 6)
  T.eq("the restore drops nothing", w._metrics.pointsDropped, 0)
  -- Order matters: the recovered batch goes to the front, ahead of what was
  -- enqueued while it was out, so a later cap eviction takes the oldest first.
  T.truthy("recovered batch sits at the front, not appended", m60.buffer[1]:find("v=1i", 1, true) ~= nil, m60.buffer[1])
  T.truthy(
    "points enqueued during the flush stay at the back",
    m60.buffer[6]:find("v=6i", 1, true) ~= nil,
    m60.buffer[6]
  )
  -- The published variable must track the restore, not read a stale zero while
  -- points pile up unsent through an outage.
  T.eq("buffered variable reflects the restore", tonumber(Variables["INFLUX_POINTS_BUFFERED"]), 6)
end)

T.finish()
