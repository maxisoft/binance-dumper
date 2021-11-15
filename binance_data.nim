import std/json
import std/strutils
import std/strformat
import std/enumerate
import std/times
import std/parseutils

proc getNodeStr(node: JsonNode): string {.inline.} =
    if node.kind == JString:
        return node.getStr()
    if node.kind == JInt:
        return $(node.getInt())
    if node.kind == JFloat:
        return $(node.getFloat())
    raise Exception.newException(fmt"{node.kind} undexpected")

proc binanceTSToTime*(timestamp: int64): Time {.inline.} =
    result = fromUnixFloat(timestamp.float / 1000.0)

proc binanceTSToTime*(timestamp: string): Time =
    var value: int64
    if parseBiggestInt(timestamp, value) == 0:
        raise Exception.newException("unable to parse timestamp")
    else:
        result = binanceTSToTime(value)

proc bookStreamToCsv*(payload: JsonNode, date: var Time): string =
    let n = payload
    assert n.kind == JObject
    assert n{"e"}.getStr() == "depthUpdate"

    let t = getNodeStr(n{"T"})
    date = binanceTSToTime(t)
    result.add(t)
    result.add(',')

    let bids = n{"b"}
    if bids != nil:
        for bid in bids:
            assert bid.kind == JArray
            assert len(bid) == 2
            result.add(getNodeStr(bid[0]))
            result.add(',')
            result.add(getNodeStr(bid[1]))
            result.add(',')

    let asks = n{"a"}
    if asks != nil:
        for ask in asks:
            assert ask.kind == JArray
            assert len(ask) == 2
            result.add(getNodeStr(ask[0]))
            result.add(',')
            result.add(getNodeStr(ask[1]))
            result.add(',')

    while len(result) > 0 and result[^1] == ',':
        result.setLen(len(result) - 1)

proc bookStreamToCsv*(payload: string, date: var Time): string {.inline} =
    let n: JsonNode = parseJson(payload, rawFloats = true, rawIntegers = true)
    return bookStreamToCsv(n, date)

proc bookStreamCsvColumns*(payload: string): seq[string] =
    var tmp: Time
    let line = bookStreamToCsv(payload, tmp)
    let splitted = line.split(',')
    doAssert len(splitted) > 0
    result.add("time")
    doAssert (len(splitted) - 1) mod 4 == 0
    for i in 0..<((len(splitted) - 1) div 4):
        result.add(fmt"bid_price_{i + 1}")
        result.add(fmt"bid_quantity_{i + 1}")

    for i in 0..<((len(splitted) - 1) div 4):
        result.add(fmt"ask_price_{i + 1}")
        result.add(fmt"ask_quantity_{i + 1}")
    

proc markPriceToCsv*(payload: JsonNode, date: var Time): string =
    let n = payload
    assert n.kind == JObject
    assert n{"e"}.getStr() == "markPriceUpdate"

    let t = getNodeStr(n{"E"})
    date = binanceTSToTime(t)
    result.add(t)
    result.add(',')

    for key in ["s", "p", "i", "P", "r"]:
        result.add(getNodeStr(n{key}))
        result.add(',')

    while len(result) > 0 and result[^1] == ',':
        result.setLen(len(result) - 1)

proc markPriceToCsv*(payload: string, date: var Time): string {.inline} =
    let n: JsonNode = parseJson(payload, rawFloats=true, rawIntegers = true)
    return markPriceToCsv(n, date)

iterator multiMarkPriceToCsv*(payload: JsonNode): (string, Time)  =
    let n = payload
    assert n.kind == JArray
    var date: Time
    for i, e in enumerate(n):
        let csv = markPriceToCsv(e, date)
        yield (csv, date)

iterator multiMarkPriceToCsv*(payload: string): (string, Time) =
    let n: JsonNode = parseJson(payload, rawFloats=true, rawIntegers = true)
    for r in multiMarkPriceToCsv(n):
        yield r

proc markPriceCsvColumns*(): array[6, string] =
    return ["time", "symbol", "mark_price", "index_price", "settle_price", "funding_rate"]