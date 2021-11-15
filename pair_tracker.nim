import asyncdispatch
import std/json
import std/times
import std/monotimes
import std/httpclient
import std/monotimes
import std/uri
import std/tables
import std/strformat
import std/strutils
import std/sets
import std/json
import binance_http
import csv_writer
import state
import std/parseutils
import std/heapqueue
import std/jsonutils

type 
    PairEntry* = object
        pair*: string
        firstSeen*: int64

    PairTracker* = ref object
        entries: OrderedTable[string, PairEntry]


proc toCsv*(self: PairEntry): string =
    result.add(self.pair)
    result.add(',')
    result.addInt(self.firstSeen)

proc exportPairJson*(self: PairTracker): JsonNode =
    var buff = newSeqOfCap[PairEntry](len(self.entries))
    for p in values(self.entries):
        buff.add(p)
    result = buff.toJson

proc newPairTracker*(): PairTracker =
    result.new()
    result.entries = initOrderedTable[string, PairEntry]()

iterator list*(self: PairTracker): PairEntry {.closure.} =
    for p in values(self.entries):
        yield p

iterator listPair*(self: PairTracker): string {.closure.} =
    for p in values(self.entries):
        yield p.pair

proc addPairEntry*(self: PairTracker, entry: PairEntry): bool {.discardable.} =
    if entry.pair in self.entries:
        return false
    self.entries[entry.pair] = entry
    return true

proc addPair*(self: PairTracker, pair: string): bool {.discardable.} =
    if pair in self.entries:
        return false
    let entry: PairEntry = PairEntry(pair: pair, firstSeen: now().utc.toTime.toUnix)
    return addPairEntry(self, entry)

proc importPairJson*(self: PairTracker, node: JsonNode) =
    let entries = to(node, seq[PairEntry])
    for e in entries:
        addPairEntry(self, e)