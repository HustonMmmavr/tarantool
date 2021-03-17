test_run = require('test_run')
inspector = test_run.new()
engine = inspector:get_cfg('engine')

--
-- Check that all modifications done to the space during index build
-- are reflected in the new index.
--
math.randomseed(os.time())

s = box.schema.space.create('test', {engine = engine})
_ = s:create_index('pk')
inspector:cmd("setopt delimiter ';'")

last_val = 1000;
box.begin()
for i = 1, last_val do
    if (i % 100 == 0) then
        box.commit()
        box.begin()
    end
    if i % 300 == 0 then
        box.snapshot()
    end
    box.space.test:replace{i, i, i}
end
box.commit();


function gen_load()
    local s = box.space.test
    for i = 1, 200 do
        local op = math.random(4)
        local key = math.random(last_val)
        local val1 = math.random(last_val)
        local val2 = last_val + 1
        last_val = val2
        if op == 1 then
            pcall(s.insert, s, {key, val1, val2})
        elseif op == 2 then
            pcall(s.replace, s, {key, val1, val2})
        elseif op == 3 then
            pcall(s.delete, s, {key})
        elseif op == 4 then
            pcall(s.upsert, s, {key, val1, val2}, {{'=', 2, val1}, {'=', 3, val2}})
        end
    end
end;

function check_equal(check, pk, k)
    if pk ~= k then
        require('log').error("Error on fiber check: failed '" .. check ..
	                     "' check on equal pk " .. pk .. " and k = " .. k)
        return false
    end
    return true
end;

function check_fiber()
    _ = fiber.create(function() gen_load() ch:put(true) end)
    _ = box.space.test:create_index('sk', {unique = false, parts = {2, 'unsigned'}})

    assert(ch:get(10) == true)

    local index = box.space.test.index
    if not check_equal("1st step secondary keys", index.pk:count(), index.sk:count()) then
        return false
    end

    _ = fiber.create(function() gen_load() ch:put(true) end)
    _ = box.space.test:create_index('tk', {unique = true, parts = {3, 'unsigned'}})

    assert(ch:get(10) == true)

    index = box.space.test.index
    if not check_equal("2nd step secondary keys", index.pk:count(), index.sk:count()) or
            not check_equal("2nd step third keys", index.pk:count(), index.tk:count()) then
        return false
    end
    return true
end;

inspector:cmd("setopt delimiter ''");

fiber = require('fiber')
ch = fiber.channel(1)
check_fiber()

inspector:cmd("restart server default")
inspector = require('test_run').new()

inspector:cmd("setopt delimiter ';'")

function check_equal(check, pk, k)
    if pk ~= k then
        require('log').error("Error on server restart check: failed '" .. check ..
                             "' check on equal pk " .. pk .. " and k = " .. k)
        return false
    end
    return true
end;

function check_server_restart()
    local index = box.space.test.index
    if not check_equal("1rd step secondary keys", index.pk:count(), index.sk:count()) or
            not check_equal("1rd step third keys", index.pk:count(), index.tk:count()) then
        return false
    end
    box.snapshot()
    index = box.space.test.index
    if not check_equal("2th step secondary keys", index.pk:count(), index.sk:count()) or
            not check_equal("2th step third keys", index.pk:count(), index.tk:count()) then
        return false
    end
    return true
end;

inspector:cmd("setopt delimiter ''");

check_server_restart()

box.space.test:drop()
