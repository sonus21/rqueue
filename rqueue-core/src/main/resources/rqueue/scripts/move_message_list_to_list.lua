--  Copyright (c) 2021-2023 Sonu Kumar
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

for i = tonumber(ARGV[1]), 1, -1
do
    local msg = redis.call('LRANGE', KEYS[1], 0, 0)[1]
    if msg ~= nil then
        redis.call("RPUSH", KEYS[2], msg)
        redis.call("LPOP", KEYS[1])
    else
        break
    end
end
local remainingMessage = redis.call("LLEN", KEYS[1])
return remainingMessage

