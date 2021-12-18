import std/re
import std/asyncfile
import std/parseutils
import std/os
import asyncdispatch

let timestampRegex = re(r"^(?:.*\s)?(\d++)\s*,(?:[^\r\n]*?,?)*\s+$", {RegexFlag.reDotAll})

proc getLastTimestampFromData(buff: string): int64 =
    result = -1
    if len(buff) > 0:
        if buff =~ timestampRegex:
            if parseBiggestInt(matches[0], result) <= 0:
                result = -3
        else:
            result = -2

proc getLastTimestamp*(file: string, sizeHint = 7): Future[int64] {.async.} =
    result = -1
    var file = openAsync(file, fmRead)
    defer:
        if file != nil:
            close(file)
    var size = file.getFileSize()
    var buff: string
    assert sizeHint >= 5
    if size > (1.int64 shl sizeHint):
        var i = sizeHint
        var pos = size - (1.int64 shl i)
        var prevResult = result
        while pos >= 0:
            file.setFilePos(pos)
            buff = await file.read((size - pos).int)
            result = getLastTimestampFromData(buff)
            if result > 0:
                if result == prevResult or pos == 0:
                    return result
                prevResult = result
            pos -= 1.int64 shl i
            if pos == -(1.int64 shl i):
                break
            pos = max(0, pos)
            inc i
            size = file.getFileSize()
    else:
        buff = await file.readAll()
        result = getLastTimestampFromData(buff)

when isMainModule:
    echo waitFor getLastTimestamp(commandLineParams()[0])