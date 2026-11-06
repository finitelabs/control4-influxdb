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

T.finish()
