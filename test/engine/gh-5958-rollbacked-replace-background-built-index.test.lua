test_run = require('test_run')
inspector = test_run.new()
fiber = require('fiber')

box.cfg{}

s = box.schema.space.create('test', {engine = 'memtx'})
i = s:create_index('pk',  {type='tree', parts={{1, 'uint'}}})

for i=1,5000 do s:replace{i, i, "valid"} end
started = false

inspector:cmd("setopt delimiter ';'");

function joinable(fib)
    fib:set_joinable(true)
    return fib
end;

function disturb()
    while not started do fiber.sleep(0) end
    box.begin()
    s:replace{0, 0, "invalid"}
    s:replace{1, 1, "changed D:"}
    i:delete{2}
    box.rollback()
end;

function create()
    started = true
    s:create_index('sk', {unique=false, type='tree', parts={{2, 'uint'}}})
end;

inspector:cmd("setopt delimiter ''");

disturber = joinable(fiber.new(disturb))
creator = joinable(fiber.new(create))

disturber:join()
creator:join()

s.index.pk:get{0} -- must be nil
s.index.sk:select{0} -- must be nil

s.index.pk:get{1}
s.index.sk:select{1} -- must be the same

s.index.pk:get{2}
s.index.sk:select{2} -- must be the same

s.index.sk:drop()

started = false

inspector:cmd("setopt delimiter ';'");

function create()
    started = true
    s:create_index('sk', {unique=true, type='tree', parts={{2, 'uint'}}})
end;

inspector:cmd("setopt delimiter ''");

disturber = joinable(fiber.new(disturb))
creator = joinable(fiber.new(create))

disturber:join()
creator:join()

s.index.pk:get{0} -- must be nil
s.index.sk:get{0} -- must be nil

s.index.pk:get{1}
s.index.sk:get{1} -- must be the same

s.index.pk:get{2}
s.index.sk:get{2} -- must be the same

s.index.sk:drop()

inspector:cmd("setopt delimiter ';'");

function disturb()
    while not started do fiber.sleep(0) end
    box.begin()
    s:replace{0, 0, "valid"}
    local svp = box.savepoint()
    s:replace{1, 1, "changed D:"}
    box.rollback_to_savepoint(svp)
    box.commit()
end;

inspector:cmd("setopt delimiter ''");

disturber = joinable(fiber.new(disturb))
creator = joinable(fiber.new(create))

disturber:join()
creator:join()

s.index.pk:get{0} -- must be valid
s.index.sk:get{0} -- must be valid

s.index.pk:get{1} -- must be valid
s.index.sk:get{1} -- must be valid

s.index.pk:delete{0}
started = false

inspector:cmd("setopt delimiter ';'");

function disturb()
    while not started do fiber.sleep(0) end
    box.begin()
    s:replace{0, 0, "invalid"}
    s:replace{1, 1, "changed D:"}
    box.rollback()
end;

function create()
    started = true
    i:alter({parts={{field = 2, type = 'unsigned'}}})
end;

inspector:cmd("setopt delimiter ''");

disturber = joinable(fiber.new(disturb))
creator = joinable(fiber.new(create))

disturber:join()
creator:join()

s.index.pk:get{0} -- must be nil
s.index.pk:get{1} -- must be valid

started = nil
s:drop()

-- Check if tuples are OK after rollback.
errinj = box.error.injection

space = box.schema.space.create('gh-4973-alter', {engine = 'memtx'})
space:format({ {'key', 'unsigned'}, {'value', 'string'}, {'key_new', 'unsigned'} })
index = space:create_index('primary', {parts = {'key'}})

N = 10000
value = string.rep('a', 10)
box.atomic(function() for i = 1, N do space:insert({i, value, i}) end end)

inspector:cmd("setopt delimiter ';'")
function random_update()
    box.begin()
    local x = space.index.primary:random(math.random(N))
    local op = math.random(10)
    if op < 10 then space:update({x[1]}, {{'=', 2, string.rep('b', 10)}}) end
    if op == 10 then space:delete({x[1]}) end
    box.rollback()
end;

finished_updates = false;
fiber = require('fiber');
updater = fiber.create(function()
    for _ = 1, N do random_update() end
    finished_updates = true
end)
inspector:cmd("setopt delimiter ''");

space.index.primary:alter({parts = {'key_new'}})
errinj.set('ERRINJ_BUILD_INDEX_DELAY', true)
inspector:wait_cond(function() return finished_updates end, 5)
errinj.set('ERRINJ_BUILD_INDEX_DELAY', false)
box.snapshot()
