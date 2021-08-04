#!/usr/bin/env tarantool

box.cfg{
    listen                 = os.getenv("LISTEN"),
    memtx_memory           = 268435456
}

require('console').listen(os.getenv('ADMIN'))
