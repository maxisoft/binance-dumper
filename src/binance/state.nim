import asyncdispatch
import std/os
import std/tables
import std/monotimes
import ./csvreader


type
    SavedTimestampState* = object
        timestamp*: string

    StateLoaderCacheEntry = object
        value: string
        modificationTime: MonoTime

    StateLoader* = ref object
        cache: Table[string, StateLoaderCacheEntry]

proc newStateLoader*(path: string): StateLoader =
    result.new()

proc getLatestFile(path: string, pattern = "*.csv"): Future[(string, int64)] {.async.} =
    var maxTime = -1.int64;
    for file in walkFiles(path & pattern):
        let t = await getLastTimestamp(file)
        if t > 0 and t > maxTime:
            result = (file, t)
            maxTime = t

proc getLastTimestamp*(self: StateLoader, pair: string, name: string, period: string): Future[int64] {.async.} =
    result = -1
    let path = $pair / $name / $period
    let (file, ts) = await getLatestFile(path)
    if len(file) > 0:
        result = ts
    elif ts != 0:
        result = ts



