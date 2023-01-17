--  Copyright (c) 2021 Sonu Kumar
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--         https://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and limitations under the License.
--

local count = redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
--if elements with lower priority are on head
local v = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if v[1] ~= nil and tonumber(v[2]) < tonumber(ARGV[3]) then
    redis.call('PUBLISH', KEYS[2], v[2])
end
return count;