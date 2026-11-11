-- The web tab's commands: lib/ui/page.js's encodeParams, run under node, reaches the driver's real handlers with
-- text in any language intact, through the newlines it breaks the body with.
--
-- Run from the driver root:
--   make test
-- or:
--   ./test/run_test.sh test_page_commands.lua

require("c4_shim")
local T = require("testlib")
local common = dofile((debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "license_gate_common.lua")

common.boot({ AC = 1, AS = "License Activated" }, 999)

local pushes = {}
function C4:SendDataToUI(command, params)
  pushes[#pushes + 1] = { command = command, data = JSON:decode(params.json) }
end

--- The last push of a command since a mark, decoded.
local function last(command, since)
  local found
  for i = (since or 0) + 1, #pushes do
    if pushes[i].command == command then
      found = pushes[i].data
    end
  end
  return found
end

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
local kit = readFile("src/lib/ui/page.js")

local function hasNode()
  local pipe = io.popen("command -v node 2>/dev/null")
  local found = pipe:read("*a") ~= ""
  pipe:close()
  return found
end

--- A command's parameters as the page's sendCommand encodes them (page.js's encodeParams), run under node.
--- @param params table
--- @return table tParams
local function fromPage(params)
  local expression = assert(kit:match("function encodeParams%(params%) {%s*return (.-);%s*}"), "no encodeParams")
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

T.section("The page sends through lib/ui, which encodes UTF-8 first")
T.check("the page embeds page.js", page:find("<!-- #embed ../../../../src/lib/ui/page.js -->", 1, true) ~= nil)
T.check(
  "which encodes UTF-8 first",
  kit:find("btoa(unescape(encodeURIComponent(JSON.stringify(params || {}))))", 1, true) ~= nil
)

T.section("Text outside Latin-1, sent through encodeParams")
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
    local mark = #pushes
    UIR._GET_CONFIG()
    local config = last("CONFIG_DATA", mark) or {}
    local stored = (config.power or { readings = {} }).readings[TEXT]
    T.neq("a reading's label is stored exactly", stored, nil)
    T.eq("and so is a literal", stored and stored.mappings.watts.literal, TEXT)
  end
else
  print("SKIP: node not installed; encodeParams was not run")
end

T.section("A property set from the page, through encodeParams")
if hasNode() then
  local mark = #pushes
  UIR._SET_PROPERTY(fromPage({ property = "Database", value = TEXT }))
  T.eq("the driver takes it", last("PROPERTY_ERROR", mark), nil)
  T.eq("and the property holds the text exactly", Properties["Database"], TEXT)
  T.eq("which the status pushed after it carries", (last("STATUS_DATA", mark) or {}).database, TEXT)
else
  print("SKIP: node not installed; encodeParams was not run")
end

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
  file:write("var Kit = { str: function (s) { return String(s); } };\n", line, "\n")
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
