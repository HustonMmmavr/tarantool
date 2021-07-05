#!/usr/bin/env tarantool
local test = require("sqltester")
test:plan(4)

local uuid = require('uuid').fromstr('11111111-1111-1111-1111-111111111111')

box.execute([[select quote(cast('11111111-1111-1111-1111-111111111111' as uuid));]])

-- Make sure that function quote() can work with uuid.
test:do_execsql_test(
    "gh-6164-1",
    [[
        SELECT quote(cast('11111111-1111-1111-1111-111111111111' as uuid));
    ]], {
        '11111111-1111-1111-1111-111111111111'
    })

-- Make sure that functions greatest() and least() can properly work with uuid.
test:do_execsql_test(
    "gh-6164-2",
    [[
        SELECT GREATEST(true, 1, x'33', cast('11111111-1111-1111-1111-111111111111' as uuid), 1e10);
    ]], {
        uuid
    })

test:do_execsql_test(
    "gh-6164-3",
    [[
        SELECT LEAST(true, 1, x'33', cast('11111111-1111-1111-1111-111111111111' as uuid), 1e10);
    ]], {
        true
    })

-- Make sure that uuid value can be binded.
box.execute('CREATE TABLE t(i INT PRIMARY KEY, a UUID);')
box.execute('INSERT INTO t VALUES(1, ?);', {uuid});

test:do_execsql_test(
    "gh-6164-4",
    [[
        SELECT * FROM t;
    ]], {
        1, uuid
    })

box.execute([[DROP TABLE t;]])

test:finish_test()
