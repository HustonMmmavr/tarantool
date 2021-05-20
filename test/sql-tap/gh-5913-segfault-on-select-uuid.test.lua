#!/usr/bin/env tarantool
local test = require("sqltester")
test:plan(3)

local uuid = require("uuid").fromstr("11111111-1111-1111-1111-111111111111")
local decimal = require("decimal").new(111.111)

box.schema.create_space('T')
box.space.T:format({{name = "I", type = "integer"}, {name = "U", type = "uuid"},
                    {name = "D", type = "decimal"}})
box.space.T:create_index("primary")
box.space.T:insert({1, uuid, decimal})

--
-- Make sure that there is no segmentation fault on select from field that
-- contains UUID or DECIMAL.
--
test:do_execsql_test(
    "gh-5913-1",
    [[
        SELECT i, u, d FROM t;
    ]], {
        1, uuid, decimal
    })

box.schema.create_space('T1')
box.space.T1:format({{name = "I", type = "integer"},
                     {name = "U", type = "uuid", is_nullable = true},
                     {name = "D", type = "decimal", is_nullable = true}})
box.space.T1:create_index("primary")

-- Make sure that INSERT and UPDATE also work properly.
test:do_execsql_test(
    "gh-5913-2",
    [[
        INSERT INTO t1 SELECT i, u, d FROM t;
        SELECT * FROM t1;
    ]], {
        1, uuid, decimal
    })

test:do_execsql_test(
    "gh-5913-3",
    [[
        UPDATE t1 SET u = u, d = d;
        SELECT * FROM t1;
    ]], {
        1, uuid, decimal
    })

test:finish_test()
