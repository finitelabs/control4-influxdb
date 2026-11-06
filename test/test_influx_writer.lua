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
local constants = require("constants")
local realPostBatch = InfluxWriter.postBatch

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
    -- Non-finite strings. tonumber parses all of these (strtod under luajit and
    -- the controller both), so before the fix the inferred/float path emitted
    -- "nan"/"inf" and the integer pin emitted "0i"/int64-max. The pair must now
    -- refuse (nil) for every type, by inference and by pin, so nothing bogus is
    -- ever serialized. A genuine word ("warm") still types as a string.
    { "nan", nil, nil },
    { "nan", "integer", nil },
    { "nan", "float", nil },
    { "inf", nil, nil },
    { "inf", "integer", nil },
    { "inf", "float", nil },
    { "-inf", nil, nil },
    { "-inf", "integer", nil },
    { "1e999", nil, nil },
    { "1e999", "integer", nil },
    { "warm", nil, '"warm"' },
  }
  for _, c in ipairs(cases) do
    local raw, pin, want = c[1], c[2], c[3]
    local vt = pin or InfluxWriter.inferValueType(raw)
    T.eq(string.format("%q as %s", raw, tostring(pin or "inferred")), InfluxWriter.formatFieldValue(raw, vt), want)
  end
end)

T.section("formatFieldValue refuses a non-finite number by any type")

-- The invariant table above feeds strings, which is what a Control4 variable
-- holds. These feed actual non-finite numbers, the shape a transform can mint,
-- so the refusal is exercised deterministically regardless of how a given Lua
-- parses "nan"/"inf". The controls confirm finite numbers still format.
local NAN = 0 / 0
local INF = math.huge
case("a non-finite number cannot be stored as any numeric or boolean field", function()
  for _, pair in ipairs({ { "a NaN", NAN }, { "positive infinity", INF }, { "negative infinity", -INF } }) do
    local label, n = pair[1], pair[2]
    for _, vt in ipairs({ "integer", "float", "boolean" }) do
      T.eq(string.format("%s as %s is refused", label, vt), InfluxWriter.formatFieldValue(n, vt), nil)
    end
  end
  -- Controls in both directions: finite numbers still format for every branch,
  -- so the guard rejects only the non-finite, not the whole numeric path.
  T.eq("a finite integer still formats", InfluxWriter.formatFieldValue(42, "integer"), "42i")
  T.eq("a finite float still formats", InfluxWriter.formatFieldValue(1.5, "float"), "1.5")
  T.eq("zero is still a false boolean", InfluxWriter.formatFieldValue(0, "boolean"), "false")
  T.eq("a nonzero number is still a true boolean", InfluxWriter.formatFieldValue(1, "boolean"), "true")
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

T.section("Driver-wide byte budget")

--- Run fn with the writer's byte budget set to `bytes`, the shipped one back afterwards.
local function withBudget(bytes, fn)
  local shipped = constants.MAX_WRITER_BUFFER_BYTES
  constants.MAX_WRITER_BUFFER_BYTES = bytes
  local ok, err = pcall(fn)
  constants.MAX_WRITER_BUFFER_BYTES = shipped
  if not ok then
    error(err, 0)
  end
end

--- Enqueue a point that always counts as changed.
local function point(w, name, i, text)
  local v = text and { value = text, type = "string" } or field(i)
  w:enqueue(name, {}, { v = v }, { interval = 60, dedup = false, dedupKey = name .. "::" .. i })
end

--- What the writer's buffers hold, in bytes.
local function bufferedBytes(w)
  local total = 0
  for _, state in pairs(w._measurements) do
    for _, line in ipairs(state.buffer) do
      total = total + #line
    end
  end
  return total
end

case("many measurements stay within the budget, and each loses its oldest points", function()
  captureWrites()
  local w = newWriter()
  withBudget(2000, function()
    for i = 1, 60 do
      for m = 1, 20 do
        point(w, "m" .. m, i)
      end
    end
  end)
  T.check("the bytes held stay within the budget", w._heldBytes <= 2000, w._heldBytes)
  T.eq("the count matches what's buffered", w._heldBytes, bufferedBytes(w))
  T.check("points were dropped", w._metrics.pointsDropped > 0, w._metrics.pointsDropped)
  local buffer = w._measurements["m1@60"].buffer
  T.truthy("the newest point is kept", buffer[#buffer]:find("v=60i", 1, true) ~= nil, buffer[#buffer])
  T.falsy("the oldest is gone", buffer[1]:find("v=1i", 1, true) ~= nil, buffer[1])
end)

case("a buffer that stopped changing can't keep a live one from holding points", function()
  captureWrites()
  local w = newWriter()
  withBudget(1000, function()
    for i = 1, 100 do
      point(w, "stale", i)
    end
    for i = 1, 10 do
      point(w, "live", i)
    end
  end)
  T.eq("the live measurement keeps its points", #w._measurements["live@60"].buffer, 10)
  T.check("the stale one gives them up", #w._measurements["stale@60"].buffer < 100, #w._measurements["stale@60"].buffer)
end)

case("a point larger than the budget goes in alone", function()
  captureWrites()
  local w = newWriter()
  withBudget(100, function()
    point(w, "m", 1)
    point(w, "m", 2, string.rep("x", 500))
    T.eq("only it remains", #w._measurements["m@60"].buffer, 1)
    T.check("and it's the large one", #w._measurements["m@60"].buffer[1] > 500, w._measurements["m@60"].buffer[1])
    point(w, "m", 3)
    T.eq("and it goes first", #w._measurements["m@60"].buffer, 1)
    T.check("leaving the budget kept", w._heldBytes <= 100, w._heldBytes)
  end)
end)

T.section("A batch in flight gives up its points too")

--- What every batch in flight holds, in bytes.
local function inFlightBytes(w)
  local total = 0
  for _, state in pairs(w._measurements) do
    for _, line in ipairs(state.inFlightBatch or {}) do
      total = total + #line
    end
  end
  return total
end

case("with whole buffers in flight, the oldest points go, not the newest", function()
  local _, pending = captureWrites()
  local w = newWriter()
  withBudget(600, function()
    for i = 1, 60 do
      point(w, "old", i)
    end
    w:forceFlushAll()
    for i = 61, 90 do
      point(w, "new", i)
    end
    T.eq("the new measurement keeps every point", #w._measurements["new@60"].buffer, 30)
    local batch = w._measurements["old@60"].inFlightBatch
    T.check("the batch out lost some", #batch < 60, #batch)
    T.falsy("from its oldest", batch[1]:find("v=1i", 1, true) ~= nil, batch[1])
    T.truthy("keeping its newest", batch[#batch]:find("v=60i", 1, true) ~= nil, batch[#batch])
    T.check("the bytes held stay within the budget", w._heldBytes <= 600, w._heldBytes)
    T.eq("the count matches what's held", w._heldBytes, bufferedBytes(w) + inFlightBytes(w))
    local kept = #batch

    settle(pending, 1, false)
    T.eq("a failure restores only what's left", #w._measurements["old@60"].buffer, kept)
    T.eq("the count still matches", w._heldBytes, bufferedBytes(w))
    T.eq("the buffered metric agrees", w._metrics.pointsBuffered, kept + 30)
  end)
end)

case("a delivery after points were taken from its batch releases only what's left", function()
  local _, pending = captureWrites()
  local w = newWriter()
  withBudget(300, function()
    for i = 1, 40 do
      point(w, "old", i)
    end
    w:forceFlushAll()
    for i = 1, 40 do
      point(w, "new", i)
    end
    settle(pending, 1)
    T.eq("the count matches what's buffered", w._heldBytes, bufferedBytes(w))
  end)
end)

case("a batch refused for good after losing points drops only what it still held", function()
  local _, pending = captureWrites()
  local w = newWriter()
  withBudget(100, function()
    for i = 1, 10 do
      point(w, "m", i)
    end
    w:forceFlushAll()
    for i = 11, 40 do
      point(w, "m", i)
    end
    local dropped, held = w._metrics.pointsDropped, #w._measurements["m@60"].inFlightBatch
    pending[1]._err({ retriable = false, errMsg = "bad token" })
    T.eq("no point counts twice", w._metrics.pointsDropped, dropped + held)
    T.eq("the count matches what's buffered", w._heldBytes, bufferedBytes(w))
  end)
end)

case("a reading that changes while the budget is in flight isn't stranded", function()
  local posts, pending = captureWrites()
  local w = newWriter()
  local function lone(value)
    w:enqueue("lone", {}, { v = field(value) }, { interval = 60, dedup = true, dedupKey = "lone" })
  end
  withBudget(600, function()
    for i = 1, 90 do
      point(w, "busy", i)
    end
    w:forceFlushAll()
    lone(42)
    for i = 91, 110 do
      point(w, "other", i)
    end
    T.eq("its point is kept", #w._measurements["lone@60"].buffer, 1)
    settle(pending, 1)
    w:forceFlushAll()
    local sent = {}
    for _, batch in ipairs(posts) do
      sent[#sent + 1] = table.concat(batch, "\n")
    end
    T.truthy("and goes out", table.concat(sent, "\n"):find("lone v=42i", 1, true) ~= nil)
  end)
end)

case("a buffer that loses its only point writes that value again", function()
  captureWrites()
  local w = newWriter()
  local function lone(value)
    w:enqueue("lone", {}, { v = field(value) }, { interval = 60, dedup = true, dedupKey = "lone" })
  end
  withBudget(40, function()
    lone(42)
    point(w, "big", 1, string.rep("x", 100))
  end)
  T.eq("its point is gone", #w._measurements["lone@60"].buffer, 0)
  lone(42)
  T.eq("and the unchanged value is buffered again", #w._measurements["lone@60"].buffer, 1)
end)

case("a batch that loses its only point, then fails, writes that value again", function()
  local _, pending = captureWrites()
  local w = newWriter()
  local function lone(value)
    w:enqueue("lone", {}, { v = field(value) }, { interval = 60, dedup = true, dedupKey = "lone" })
  end
  withBudget(40, function()
    lone(42)
    w:forceFlushAll()
    point(w, "big", 1, string.rep("x", 100))
  end)
  T.eq("the batch out has nothing left", #w._measurements["lone@60"].inFlightBatch, 0)
  settle(pending, 1, false)
  CancelTimer(w._measurements["lone@60"].timerName)
  w._measurements["lone@60"].timerName = nil
  lone(42)
  T.eq("the unchanged value is buffered again", #w._measurements["lone@60"].buffer, 1)
end)

case("a long-running request at the ceiling keeps the newest points of every reading", function()
  local _, pending = captureWrites()
  local w = newWriter()
  local answered = 0
  withBudget(4000, function()
    for tick = 1, 300 do
      for m = 1, 57 do
        point(w, "m" .. m, tick)
      end
      -- Requests that last two ticks: what's out now is answered, with a failure, two ticks later.
      if tick % 2 == 0 then
        w:forceFlushAll()
        while answered + 57 < #pending do
          answered = answered + 1
          settle(pending, answered, false)
          for _, state in pairs(w._measurements) do
            if state.timerName then
              CancelTimer(state.timerName)
              state.timerName = nil
            end
          end
        end
      end
    end
  end)
  local kept = 0
  for m = 1, 57 do
    for _, line in ipairs(w._measurements["m" .. m .. "@60"].buffer) do
      if line:find("v=300i", 1, true) then
        kept = kept + 1
      end
    end
  end
  T.eq("the last tick's points are all held", kept, 57)
  T.check("the bytes held stay within the budget", w._heldBytes <= 4000, w._heldBytes)
end)

T.section("What the budget counts")

case("a batch in flight counts until it's delivered", function()
  local _, pending = captureWrites()
  local w = newWriter()
  point(w, "m", 1)
  point(w, "m", 2)
  local bytes = w._heldBytes
  T.check("the points count", bytes > 0, bytes)
  w:forceFlushAll()
  T.eq("in flight, they still count", w._heldBytes, bytes)
  T.eq("the buffer's own count is empty", w._measurements["m@60"].bufferBytes, 0)
  settle(pending, 1)
  T.eq("delivered, they don't", w._heldBytes, 0)
end)

case("a failed batch still counts, and its retry's delivery clears it", function()
  local _, pending = captureWrites()
  local w = newWriter()
  point(w, "m", 1)
  point(w, "m", 2)
  local bytes = w._heldBytes
  w:forceFlushAll()
  settle(pending, 1, false)
  T.eq("restored, it still counts", w._heldBytes, bytes)
  T.eq("the buffer's count has it back", w._measurements["m@60"].bufferBytes, bytes)
  CancelTimer(w._measurements["m@60"].timerName)
  w._measurements["m@60"].timerName = nil
  w:forceFlushAll()
  settle(pending, 2)
  T.eq("delivered, nothing counts", w._heldBytes, 0)
end)

case("a batch the server refuses for good stops counting", function()
  local _, pending = captureWrites()
  local w = newWriter()
  point(w, "m", 1)
  w:forceFlushAll()
  pending[1]._err({ retriable = false, errMsg = "bad token" })
  T.eq("nothing counts", w._heldBytes, 0)
end)

case("the watchdog's reissue doesn't count a batch twice", function()
  local _, pending = captureWrites()
  local w = newWriter()
  point(w, "m", 1)
  w:forceFlushAll()
  point(w, "m", 2)
  local bytes = w._heldBytes
  local state = w._measurements["m@60"]
  state.lastFlushTime = os.time() - 100000
  w:forceFlushAll()
  T.eq("two requests, one count", w._heldBytes, bytes)
  settle(pending, 1) -- the lost request answers after all
  T.eq("the superseded answer changes nothing", w._heldBytes, bytes)
  settle(pending, 2)
  T.eq("the reissue's delivery clears it", w._heldBytes, 0)
end)

case("a removed measurement stops counting, even with a request that never answers", function()
  local _, pending = captureWrites()
  local w = newWriter()
  point(w, "m", 1)
  w:forceFlushAll()
  point(w, "m", 2)
  w:removeMeasurement("m")
  T.eq("buffered and in flight are both released", w._heldBytes, 0)
  settle(pending, 1, false)
  T.eq("a late failure doesn't count it again", w._heldBytes, 0)
  T.eq("and doesn't arm a retry", w._metrics.writeErrors, 0)
end)

T.section("After points were lost")

case("a reading whose latest point was lost writes its value again once delivery resumes", function()
  local posts, pending = captureWrites()
  local w = newWriter()
  local function reading(name, value)
    w:enqueue("m", {}, { v = field(value) }, { interval = 60, dedup = true, dedupKey = name })
  end
  withBudget(40, function()
    reading("steady", 5)
    for i = 100, 140 do
      reading("busy", i)
    end
  end)
  T.falsy("steady's point was lost", table.concat(w._measurements["m@60"].buffer, " "):find("v=5i", 1, true) ~= nil)
  w:forceFlushAll()
  settle(pending, 1)
  reading("steady", 5)
  T.truthy("its unchanged value is buffered again", w._measurements["m@60"].buffer[1] ~= nil)
  reading("steady", 5)
  T.eq("once: dedup holds after that", #w._measurements["m@60"].buffer, 1)
  T.eq("what was buffered was delivered first", #posts, 1)
end)

case("a reading with nothing lost is not written again", function()
  local _, pending = captureWrites()
  local w = newWriter()
  local function reading(value)
    w:enqueue("m", {}, { v = field(value) }, { interval = 60, dedup = true, dedupKey = "steady" })
  end
  reading(5)
  w:forceFlushAll()
  settle(pending, 1)
  reading(5)
  T.eq("dedup still skips it", #w._measurements["m@60"].buffer, 0)
end)

T.section("Connection status")

local function statusWriter(extra)
  local seen = {}
  local w = InfluxWriter:new({
    getConfig = function()
      return { url = "http://influx.test", database = "db", precision = "ms", token = "t" }
    end,
    onConnected = function(connected)
      seen[#seen + 1] = connected
    end,
    onBufferFull = extra,
  })
  return w, seen
end

case("a failure that may clear reports disconnected, and delivery reports connected", function()
  local _, pending = captureWrites()
  local w, seen = statusWriter()
  point(w, "m", 1)
  w:forceFlushAll()
  settle(pending, 1, false)
  T.eq("disconnected", seen, { false })
  CancelTimer(w._measurements["m@60"].timerName)
  w._measurements["m@60"].timerName = nil
  w:forceFlushAll()
  settle(pending, 2)
  T.eq("then connected", seen, { false, true })
end)

case("a rate limit isn't a disconnection: the server answered", function()
  local _, pending = captureWrites()
  local w, seen = statusWriter()
  point(w, "m", 1)
  w:forceFlushAll()
  pending[1]._err({ retriable = true, reachable = true, errMsg = "rate limited (HTTP 429)" })
  T.eq("no status change", seen, {})
  CancelTimer(w._measurements["m@60"].timerName)
end)

case("postBatch says which failures the server answered", function()
  local answer
  local realUrlPost = C4.urlPost
  function C4:urlPost(_, _, _, _, callback)
    callback(1, "", answer.code, {}, answer.err)
  end
  local function reject(code, err)
    answer = { code = code, err = err }
    local out
    realPostBatch("http://influx.test", "t", { "m v=1" }):next(nil, function(e)
      out = e
    end)
    return out
  end
  T.eq("a 429 was answered", reject(429).reachable, true)
  T.eq("and may clear", reject(429).retriable, true)
  T.eq("a 503 wasn't", reject(503).reachable, nil)
  T.eq("nor a network error", reject(0, "refused").reachable, nil)
  C4.urlPost = realUrlPost
end)

T.section("Buffer Full")

case("it fires once per outage, however many points are lost", function()
  local _, pending = captureWrites()
  local fired = 0
  local w = statusWriter(function()
    fired = fired + 1
  end)
  withBudget(200, function()
    for i = 1, 50 do
      point(w, "m", i)
    end
    T.check("points were lost", w._metrics.pointsDropped > 1, w._metrics.pointsDropped)
    T.eq("it fired once", fired, 1)
    w:forceFlushAll()
    settle(pending, 1)
    for i = 51, 100 do
      point(w, "m", i)
    end
    T.eq("and again after delivery resumed", fired, 2)
  end)
end)

T.section("A lost request")

case("the watchdog's reissue leaves nothing holding the lost request's lines", function()
  local callbacks = {}
  local realUrlPost = C4.urlPost
  function C4:urlPost(_, _, _, _, callback)
    callbacks[#callbacks + 1] = callback
  end
  InfluxWriter.postBatch = realPostBatch
  local w = newWriter()
  for i = 1, 5 do
    point(w, "m", i)
  end
  w:forceFlushAll()
  local state = w._measurements["m@60"]
  local weak = setmetatable({ batch = state.inFlightBatch }, { __mode = "v" })
  point(w, "m", 6)
  state.lastFlushTime = os.time() - 100000
  w:forceFlushAll()
  collectgarbage()
  collectgarbage()
  T.eq("two requests went out", #callbacks, 2)
  T.eq("the first batch is released", weak.batch, nil)

  callbacks[2](2, "", 204, {}, nil)
  T.eq("the reissue delivers", w._metrics.pointsWritten, 6)
  callbacks[1](1, "", 503, {}, nil)
  T.eq("and the lost one's late failure changes nothing", #state.buffer, 0)
  C4.urlPost = realUrlPost
end)

T.finish()
