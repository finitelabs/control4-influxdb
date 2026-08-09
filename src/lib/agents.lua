--- Project agent lookup for the measurement device picker.
---
--- Agents are absent from C4:GetDevices(), so mapping a variable that lives on
--- one is otherwise impossible from the UI.

local log = require("lib.logging")

--- @class Agents
local Agents = {}

--- Agents Director exposes that no GetProjectItems filter lists. Included only
--- when they really resolve variables, so a dead id leaves no picker entry.
--- @type table<number, string>
local SYSTEM_AGENTS = {
  [100001] = "Variables",
}

--- @type table<number, string>|nil
local cache

--- Project agents, keyed by device id.
---
--- "AGENTS" is a real filter, roughly 160KB against 15MB. An unrecognised token
--- silently returns everything, so it is spelled as GetAgentId spells it.
--- @param refresh boolean|nil Rebuild rather than reusing the cached result.
--- @return table<number, string> agents id -> name
function Agents.getAll(refresh)
  log:trace("Agents.getAll(%s)", tostring(refresh))
  if cache and not refresh then
    return cache
  end

  local agents = {}
  for id, name in pairs(SYSTEM_AGENTS) do
    local ok, vars = pcall(C4.GetDeviceVariables, C4, id)
    if ok and vars ~= nil and next(vars) ~= nil then
      agents[id] = name
    end
  end

  -- Parsed, not pattern-matched: items nest, so a non-greedy <item>(.-)</item>
  -- ends at the first child and reads the parent's id against a child's type.
  local ok, parsed = pcall(function()
    return Select(ParseXml(C4:GetProjectItems("AGENTS")), "systemitems", "item")
  end)
  if ok then
    -- Keyed on ok, not parsed: no agents parses to nil, which is a successful
    -- read of nothing. Treating it as failure reparses 58KB on every call.
    if parsed then
      if not IsList(parsed) then
        parsed = { parsed }
      end
      for _, agent in pairs(parsed) do
        local id = tointeger(not IsEmpty(agent) and agent.id or nil)
        if id and not IsEmpty(agent.name) then
          agents[id] = agent.name
        end
      end
    end
    cache = agents
  else
    -- Not cached on failure, which would pin it to SYSTEM_AGENTS for good.
    log:warn("Agents.getAll: could not read the agent list")
  end

  return agents
end

--- Look up one agent's name.
--- @param deviceId number|string
--- @return string|nil name
function Agents.name(deviceId)
  local id = tointeger(deviceId)
  return id and Agents.getAll()[id] or nil
end

--- Drop the cached list so a newly added agent can be picked without a restart.
function Agents.invalidate()
  cache = nil
end

return Agents
