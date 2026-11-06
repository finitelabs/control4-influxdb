-- The Settings tab's commands: a page's own sendCommand line, run under node, reaches the driver's real handlers with
-- text in any language intact.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_page_commands.lua

require("c4_shim")
local T = require("testlib")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local ROOT = (debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "../"

--- Text from every length of UTF-8, with what XML reads as markup.
local TEXT = "caf\195\169 \230\151\165\230\156\172 \240\159\146\161 <a&b>"

local function readFile(path)
  local fh = assert(io.open(ROOT .. path, "r"))
  local body = fh:read("*a")
  fh:close()
  return body
end

local page = readFile("drivers/influxdb/www/html/index.html")

local function hasNode()
  local pipe = io.popen("command -v node 2>/dev/null")
  local found = pipe:read("*a") ~= ""
  pipe:close()
  return found
end

--- A command's parameters as the page's own sendCommand line encodes them, run under node.
--- @param params table
--- @return table tParams
local function fromPage(params)
  local expression = assert(page:match("DATA: (.-) },\n"), "no DATA line in the page")
  local path = os.tmpname()
  local file = assert(io.open(path, "w"))
  file:write("const params = ", JSON:encode(params), ";\nprocess.stdout.write(", expression, ");\n")
  file:close()
  local pipe = io.popen(string.format("node '%s' 2>/dev/null", path))
  local data = pipe:read("*a")
  pipe:close()
  os.remove(path)
  return { DATA = data }
end

T.section("The page encodes UTF-8 first")
T.check("its line does", page:find("btoa(unescape(encodeURIComponent(JSON.stringify(params || {}))))", 1, true) ~= nil)

T.section("Text outside Latin-1, sent by the page's own line")
if hasNode() then
  local reading = fromPage({ measurement = "power", label = TEXT })
  T.check("the page encodes it", reading.DATA ~= "")
  if reading.DATA ~= "" then
    UIR._ADD_READING(reading)
    UIR._SAVE_MAPPING(fromPage({
      measurement = "power",
      reading = TEXT,
      name = "watts",
      mapping = { source = "literal", literal = TEXT },
    }))
    local config = JSON:decode(JSON:decode(UIR._GET_CONFIG()).config)
    local stored = config.power.readings[TEXT]
    T.neq("a reading's label is stored exactly", stored, nil)
    T.eq("and so is a literal", stored and stored.mappings.watts.literal, TEXT)
  end
else
  print("SKIP: node not installed; the page's own line was not run")
end

T.section("A property set encodes what Director reads as XML")
T.check(
  "the page's setProperty does",
  page:find("String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;')", 1, true) ~= nil
)

T.section("Names outside Latin-1 keep their own ids")
if hasNode() then
  local line = assert(page:match("(function escId%(s%).-)\n"), "no escId in the page")
  local names = {
    "\208\154\209\131\209\133\208\189\209\143",
    "\208\146\208\176\208\189\208\189\208\176",
    "\230\151\165\230\156\172",
    "\230\184\169\229\186\166",
    "a_b",
    "a b",
    "a__b",
  }
  local path = os.tmpname()
  local file = assert(io.open(path, "w"))
  file:write("function str(s) { return String(s); }\n", line, "\n")
  file:write("process.stdout.write(JSON.stringify(", JSON:encode(names), ".map(escId)));\n")
  file:close()
  local pipe = io.popen(string.format("node '%s' 2>/dev/null", path))
  local ids = JSON:decode(pipe:read("*a"))
  pipe:close()
  os.remove(path)
  local seen = {}
  for i, id in ipairs(ids) do
    T.check(names[i] .. " has an id of its own", not seen[id], id)
    T.check(names[i] .. "'s id can't split", not id:find("__", 1, true), id)
    T.check(names[i] .. "'s id is safe in markup", id:match("^[%w_]+$") ~= nil, id)
    seen[id] = true
  end
else
  print("SKIP: node not installed; the page's ids were not run")
end

T.finish()
