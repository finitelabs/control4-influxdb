-- Shared by the license gate tests: loads the DriverCentral build of the driver with a fake Cloud driver answering,
-- configures one measurement through the web tab, and records every HTTP request the driver makes.

local M = { SENSOR = 500 }
local ROOT = (debug.getinfo(1, "S").source:match("@(.*[/\\])") or "./") .. "../"

--- A build directive on a line of Lua (`--#ifdef X`) or XML (`<!-- #ifdef X -->`), and the name it tests.
--- @param line string
--- @return string|nil directive ifdef, ifndef, else or endif
--- @return string|nil name
local function directive(line)
  local body = line:match("^%s*%-%-%s*(#.-)%s*$") or line:match("^%s*<!%-%-%s*(#.-)%s*%-%->%s*$")
  if not body then
    return nil
  end
  local ifdef, name = body:match("^#(ifn?def)%s+([%w_]+)$")
  if ifdef then
    return ifdef, name
  end
  return body:match("^#(else)$") or body:match("^#(endif)$")
end

--- The text of `path` as the DriverCentral build has it: a line its directives leave out is blanked.
--- @param path string
--- @return string
local function driverCentralText(path)
  local f = assert(io.open(path))
  local lines, stack, including = {}, {}, true
  for line in f:lines() do
    local kind, name = directive(line)
    if kind == "ifdef" or kind == "ifndef" then
      stack[#stack + 1] = including
      including = including and (name == "DRIVERCENTRAL") == (kind == "ifdef")
    elseif kind == "else" then
      including = stack[#stack] and not including
    elseif kind == "endif" then
      including = table.remove(stack)
    end
    lines[#lines + 1] = including and line or ""
  end
  f:close()
  return table.concat(lines, "\n")
end

--- Runs `path` as the DriverCentral build compiles it.
local function dofileDriverCentral(path)
  assert(load(driverCentralText(path), "@" .. path))()
end

--- Properties as Director creates them from driver.xml's defaults.
local function properties(overrides)
  local f = assert(io.open(ROOT .. "drivers/influxdb/driver.xml"))
  local xml = f:read("*a")
  f:close()
  for block in xml:gmatch("<property>(.-)</property>") do
    Properties[block:match("<name>(.-)</name>")] = block:match("<default>(.-)</default>") or ""
  end
  for k, v in pairs(overrides) do
    Properties[k] = v
  end
end

local function data(t)
  return { DATA = C4:Base64Encode(JSON:encode(t)) }
end

--- Loads the driver with the Cloud driver answering `answer` and `pid` as its product, and returns the list every
--- HTTP request lands in.
--- @param answer table
--- @param pid integer
--- @return string[] posts
function M.boot(answer, pid)
  local cloud = require("dc_cloud")
  cloud.answer = answer
  local posts = {}
  function C4:urlPost(url, _, _, _, callback)
    posts[#posts + 1] = "POST " .. tostring(url)
    if callback then
      callback(1, "", 204, {}, nil)
    end
    return 1
  end
  function C4:UpdateProperty(name, value)
    Properties[name] = value
  end
  local configInfo = C4.GetDriverConfigInfo
  local config = driverCentralText(ROOT .. "drivers/influxdb/driver.xml")
  function C4:GetDriverConfigInfo(section)
    if section == "version" then
      return "20261104"
    elseif section == "config" then
      return config
    end
    return configInfo(self, section)
  end
  C4.SendDataToUI = function() end
  C4.FireEvent = function() end
  ShimSetDevices({
    [M.SENSOR] = { deviceName = "Meter", driverFileName = "meter.c4z", roomId = "1", roomName = "Office" },
  })
  properties({
    ["InfluxDB URL"] = "http://198.51.100.20:8181",
    ["Database"] = "home",
    ["API Token"] = "throwaway",
    ["Cloud Status"] = "",
    ["Automatic Updates"] = "Off",
  })
  require("lib.drivercentral").PRODUCT_ID = pid
  dofileDriverCentral(ROOT .. "drivers/influxdb/driver.lua")
  OnDriverInit()
  -- lib.http sends through the global urlDo, which the driver's requires define.
  urlDo = function(method, url, _, _, callback)
    posts[#posts + 1] = tostring(method) .. " " .. tostring(url)
    if callback then
      callback(nil, 204, {}, "", nil, url)
    end
  end
  OnDriverLateInit("DIT_STARTUP")
  UIRequest("_ADD_MEASUREMENT", data({ name = "power" }))
  UIRequest("_ADD_FIELD_DEF", data({ measurement = "power", name = "watts" }))
  UIRequest("_ADD_READING", data({ measurement = "power", label = "main" }))
  UIRequest("_SAVE_MAPPING", {
    DATA = data({
      measurement = "power",
      reading = "main",
      name = "watts",
      mapping = { source = "variable", varId = M.SENSOR .. ":1000" },
    }).DATA,
  })
  return posts
end

--- Loads the driver again after a Director restart: no variable survives, and its modules start fresh over what
--- they stored.
--- @param pid integer
function M.restart(pid)
  ShimRestartDirector()
  for name in pairs(package.loaded) do
    if name:match("^lib%.") or name == "constants" then
      package.loaded[name] = nil
    end
  end
  require("lib.drivercentral").PRODUCT_ID = pid
  dofileDriverCentral(ROOT .. "drivers/influxdb/driver.lua")
  OnDriverInit()
  OnDriverLateInit("DIT_STARTUP")
end

--- Whether a write to InfluxDB landed in `posts` after `mark`.
--- @param posts string[]
--- @param mark integer
--- @return boolean
function M.wrote(posts, mark)
  for i = mark + 1, #posts do
    if posts[i]:find("/api/v2/write", 1, true) then
      return true
    end
  end
  return false
end

return M
