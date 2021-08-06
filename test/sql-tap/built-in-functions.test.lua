#!/usr/bin/env tarantool
local test = require("sqltester")
test:plan(42)

--
-- Make sure that number of arguments check is checked properly for SQL built-in
-- functions with variable number of arguments.
--
test:do_catchsql_test(
    "builtins-1.1",
    [[
        SELECT COUNT(1, 2);
    ]],
    {
        1, [[Wrong number of arguments is passed to COUNT(): ]]..
           [[expected from 0 to 1, got 2]]
    }
)

test:do_catchsql_test(
    "builtins-1.2",
    [[
        SELECT GREATEST();
    ]],
    {
        1, [[Wrong number of arguments is passed to GREATEST(): ]]..
           [[expected at least 2, got 0]]
    }
)

test:do_catchsql_test(
    "builtins-1.3",
    [[
        SELECT GROUP_CONCAT();
    ]],
    {
        1, [[Wrong number of arguments is passed to GROUP_CONCAT(): ]]..
           [[expected from 1 to 2, got 0]]
    }
)

test:do_catchsql_test(
    "builtins-1.4",
    [[
        SELECT GROUP_CONCAT(1, 2, 3);
    ]],
    {
        1, [[Wrong number of arguments is passed to GROUP_CONCAT(): ]]..
           [[expected from 1 to 2, got 3]]
    }
)

test:do_catchsql_test(
    "builtins-1.5",
    [[
        SELECT LEAST();
    ]],
    {
        1, [[Wrong number of arguments is passed to LEAST(): ]]..
           [[expected at least 2, got 0]]
    }
)

test:do_catchsql_test(
    "builtins-1.6",
    [[
        SELECT ROUND();
    ]],
    {
        1, [[Wrong number of arguments is passed to ROUND(): ]]..
           [[expected from 1 to 2, got 0]]
    }
)

test:do_catchsql_test(
    "builtins-1.7",
    [[
        SELECT ROUND(1, 2, 3);
    ]],
    {
        1, [[Wrong number of arguments is passed to ROUND(): ]]..
           [[expected from 1 to 2, got 3]]
    }
)

test:do_catchsql_test(
    "builtins-1.8",
    [[
        SELECT SUBSTR('1');
    ]],
    {
        1, [[Wrong number of arguments is passed to SUBSTR(): ]]..
           [[expected from 2 to 3, got 1]]
    }
)

test:do_catchsql_test(
    "builtins-1.9",
    [[
        SELECT SUBSTR('1', '2', '3', '4');
    ]],
    {
        1, [[Wrong number of arguments is passed to SUBSTR(): ]]..
           [[expected from 2 to 3, got 4]]
    }
)

test:do_catchsql_test(
    "builtins-1.10",
    [[
        SELECT UUID(1, 2);
    ]],
    {
        1, [[Wrong number of arguments is passed to UUID(): ]]..
           [[expected from 0 to 1, got 2]]
    }
)

-- Make sure static and dynamic argument type checking is working correctly.

test:do_catchsql_test(
    "builtins-2.1",
    [[
        SELECT CHAR_LENGTH(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function CHAR_LENGTH()]]
    }
)

test:do_test(
    "builtins-2.2",
    function()
        local res = {pcall(box.execute, [[SELECT CHAR_LENGTH(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.3",
    [[
        SELECT CHARACTER_LENGTH(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function CHARACTER_LENGTH()]]
    }
)

test:do_test(
    "builtins-2.4",
    function()
        local res = {pcall(box.execute, [[SELECT CHARACTER_LENGTH(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.5",
    [[
        SELECT CHAR('1');
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function CHAR()]]
    }
)

test:do_test(
    "builtins-2.6",
    function()
        local res = {pcall(box.execute, [[SELECT CHAR(?);]], {'1'})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert string('1') to integer"
    })

test:do_catchsql_test(
    "builtins-2.7",
    [[
        SELECT HEX(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function HEX()]]
    }
)

test:do_test(
    "builtins-2.8",
    function()
        local res = {pcall(box.execute, [[SELECT HEX(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to varbinary"
    })

test:do_catchsql_test(
    "builtins-2.9",
    [[
        SELECT LENGTH(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function LENGTH()]]
    }
)

test:do_test(
    "builtins-2.10",
    function()
        local res = {pcall(box.execute, [[SELECT LENGTH(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.11",
    [[
        SELECT 1 LIKE '%';
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function LIKE()]]
    }
)

test:do_test(
    "builtins-2.12",
    function()
        local res = {pcall(box.execute, [[SELECT ? LIKE '%';]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.13",
    [[
        SELECT LOWER(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function LOWER()]]
    }
)

test:do_test(
    "builtins-2.14",
    function()
        local res = {pcall(box.execute, [[SELECT LOWER(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.15",
    [[
        SELECT UPPER(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function UPPER()]]
    }
)

test:do_test(
    "builtins-2.16",
    function()
        local res = {pcall(box.execute, [[SELECT UPPER(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.17",
    [[
        SELECT POSITION(1, 1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function POSITION()]]
    }
)

test:do_test(
    "builtins-2.18",
    function()
        local res = {pcall(box.execute, [[SELECT POSITION(?, ?);]], {1, 1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.19",
    [[
        SELECT RANDOMBLOB('1');
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function RANDOMBLOB()]]
    }
)

test:do_test(
    "builtins-2.20",
    function()
        local res = {pcall(box.execute, [[SELECT RANDOMBLOB(?);]], {'1'})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert string('1') to integer"
    })

test:do_catchsql_test(
    "builtins-2.21",
    [[
        SELECT ZEROBLOB('1');
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function ZEROBLOB()]]
    }
)

test:do_test(
    "builtins-2.22",
    function()
        local res = {pcall(box.execute, [[SELECT ZEROBLOB(?);]], {'1'})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert string('1') to integer"
    })

test:do_catchsql_test(
    "builtins-2.23",
    [[
        SELECT SOUNDEX(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function SOUNDEX()]]
    }
)

test:do_test(
    "builtins-2.24",
    function()
        local res = {pcall(box.execute, [[SELECT SOUNDEX(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.25",
    [[
        SELECT UNICODE(1);
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function UNICODE()]]
    }
)

test:do_test(
    "builtins-2.26",
    function()
        local res = {pcall(box.execute, [[SELECT UNICODE(?);]], {1})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert integer(1) to string"
    })

test:do_catchsql_test(
    "builtins-2.27",
    [[
        SELECT ABS('1');
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function ABS()]]
    }
)

test:do_test(
    "builtins-2.28",
    function()
        local res = {pcall(box.execute, [[SELECT ABS(?);]], {'1'})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert string('1') to integer"
    })

test:do_catchsql_test(
    "builtins-2.29",
    [[
        SELECT ROUND('1');
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function ROUND()]]
    }
)

test:do_test(
    "builtins-2.30",
    function()
        local res = {pcall(box.execute, [[SELECT ROUND(?);]], {'1'})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert string('1') to double"
    })

test:do_catchsql_test(
    "builtins-2.31",
    [[
        SELECT UUID('1');
    ]],
    {
        1, [[Failed to execute SQL statement: ]]..
           [[wrong arguments for function UUID()]]
    }
)

test:do_test(
    "builtins-2.32",
    function()
        local res = {pcall(box.execute, [[SELECT UUID(?);]], {'1'})}
        return {tostring(res[3])}
    end, {
        "Type mismatch: can not convert string('1') to integer"
    })

test:finish_test()
