import asyncdispatch
import ws
import os
import std/sets
import std/json
import std/jsonutils
import std/os
import std/strformat
import std/math
import std/times
import std/monotimes
import std/deques
import std/asyncfile
import std/tables
import std/options
import std/asyncfile


type
  SavedTimestampState* = object
    timestamp*: string

type
  State* = object
    version*: int64
    schema_version*: int
    openInterestHist*: Table[string, SavedTimestampState]
    topLongShortAccountRatio*: Table[string, SavedTimestampState]
    topLongShortPositionRatio*: Table[string, SavedTimestampState]
    globalLongShortAccountRatio*: Table[string, SavedTimestampState]
    takerlongshortRatio*: Table[string, SavedTimestampState]


const LATEST_SCHEMA_VERSION = 1

proc newState*(): State =
    result.schema_version = LATEST_SCHEMA_VERSION
    result.version = 0

proc incrementVersion*(self: var State) =
    inc self.version

proc compareVersion*(self: var State, version: int64): int64 =
    return self.version - version

type
  StateLoader* = ref object
    path: string
    modificationTime: Option[times.Time]
    latest: State
    fd: Option[AsyncFile]

proc save*(loader: StateLoader) {.async.} = 
    let payload = loader.latest.toJson
    var file: AsyncFile
    var fileOwned = false
    if loader.fd.isSome:
        file = loader.fd.get()
        fileOwned = true
    else:
        var fm = fmReadWriteExisting
        if not fileExists(loader.path):
            fm = fmReadWrite
        file = openAsync(loader.path, fm)
    try:
        file.setFilePos(0)
        var buff: string
        toUgly(buff, payload)
        await file.write(buff)
        if file.getFileSize() > file.getFilePos():
            file.setFileSize(file.getFilePos()) # note: this call may flush the fd
    except:
        if not fileOwned:
            file.close()
        raise
    if not fileOwned:
        loader.fd = file.some()
    loader.modificationTime = getLastModificationTime(loader.path).some()

proc hasNewState*(loader: StateLoader): bool =
    if not loader.modificationTime.isSome:
        return fileExists(loader.path)
    if loader.path != "" and fileExists(loader.path):
        return loader.modificationTime.get() < getLastModificationTime(loader.path)
    return false

proc load*(loader: StateLoader) {.async.} = 
    var file: AsyncFile
    var fileOwned = false
    if loader.fd.isSome:
        file = loader.fd.get()
        fileOwned = true
    else:
        var fm = fmReadWriteExisting
        if not fileExists(loader.path):
            fm = fmReadWrite
        file = openAsync(loader.path, fm)
    try:
        file.setFilePos(0)
        let data = await file.readAll()
        if len(data) > 0:
            let payload = parseJson(data)
            loader.latest = to(payload, State)
    except:
        if not fileOwned:
            file.close()
        raise
    if not fileOwned:
        loader.fd = file.some()

proc get*(loader: StateLoader): var State =
    return loader.latest

proc close*(loader: StateLoader) =
    if loader.fd.isSome:
        loader.fd.get().close()
        loader.fd = AsyncFile.none()

proc newStateLoader*(path: string): StateLoader =
    when defined(gcOrc) or defined(gcArc):
        result.new()
    else:
        result.new(close)
    result.path = path
    result.latest = newState()
    result.fd = AsyncFile.none()



