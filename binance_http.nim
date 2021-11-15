import asyncdispatch
import std/json
import std/times
import std/monotimes
import std/httpclient
import std/monotimes
import std/uri
import std/tables
import std/parseutils


type 
    BinanceRateLimiter* = ref object
        weight: int64
        tick: MonoTime

    BinanceRateLimiterSingleton = object
        rl: BinanceRateLimiter
        latestRequestLimit: int

    BinanceHttpClient* = ref object
        client*: AsyncHttpClient
    
    BaseBinanceHistorycalEntry* {.inheritable.} = ref object of RootObj
        symbol*: string
        timestamp*: string

    OpenInterestHist* = ref object of BaseBinanceHistorycalEntry
        sumOpenInterest*: string
        sumOpenInterestValue*: string

    BaseLongShortRatio* {.inheritable.} = ref object of BaseBinanceHistorycalEntry
        longShortRatio*: string
        longAccount*: string
        shortAccount*: string

    TopTraderLongShortRatioAccounts* = ref object of BaseLongShortRatio
    TopTraderLongShortRatioPositions* = ref object of BaseLongShortRatio
    LongShortRatio* = ref object of BaseLongShortRatio

var rateLimiterSingleton = BinanceRateLimiterSingleton()

proc rateLimiter(self: var BinanceRateLimiterSingleton): var BinanceRateLimiter =
    if self.rl.isNil:
        # TODO lock pattern ?
        self.rl.new()
        self.rl.tick = getMonoTime()
        self.latestRequestLimit = 0
    result = self.rl

proc rateLimiter*(): var BinanceRateLimiter =
    result = rateLimiterSingleton.rateLimiter

method toCsv*(this: BaseBinanceHistorycalEntry, includeSymbol = false): string {.base.} =
    raise Exception.newException("must be implemented")

method date*(this: BaseBinanceHistorycalEntry): Time {.base.} =
    var value: int64
    if parseBiggestInt(this.timestamp, value) == 0:
        raise Exception.newException("unable to parse timestamp")
    else:
        result = fromUnixFloat(value.float / 1000.0)

method toCsv*(this: OpenInterestHist, includeSymbol = false): string =
    result.add(this.timestamp)
    if includeSymbol:
        result.add(',')
        result.add(this.symbol)
    result.add(',')
    result.add(this.sumOpenInterest)
    result.add(',')
    result.add(this.sumOpenInterestValue)


method toCsv*(this: BaseLongShortRatio, includeSymbol = false): string =
    result.add(this.timestamp)
    if includeSymbol:
        result.add(',')
        result.add(this.symbol)
    result.add(',')
    result.add(this.longShortRatio)
    result.add(',')
    result.add(this.longAccount)
    result.add(',')
    result.add(this.shortAccount)

const 
    USER_AGENT = "nim/1.0"
    BASE_URL = "https://fapi.binance.com"
    RATE_LIMITER_DEFAULT_LIMIT = 600 # 2400 last time checked
    RATE_LIMITER_LIMIT_SECURITY_FACTOR = 0.2 # FIXME for now we limit the request/second even more (avoiding bans)
    RATE_LIMITER_SLEEP_TICK = 50 # ms
    oneMinute = initDuration(minutes = 1)

proc createAsyncHttpClient(): AsyncHttpClient =
    result = newAsyncHttpClient(userAgent=USER_AGENT)


proc finalizer*(x: BinanceHttpClient) =
  if x.client != nil:
    x.client.close()

proc newBinanceHttpClient*(): BinanceHttpClient =
    when defined(gcOrc) or defined(gcArc):
        result.new()
    else:
        result.new(finalizer)
    result.client = createAsyncHttpClient()

proc waitForLimit*(self: BinanceRateLimiter, limit = -1, timeout = -1) {.async.} =
    var l = limit
    if limit == -1:
        l = RATE_LIMITER_DEFAULT_LIMIT
        if rateLimiterSingleton.latestRequestLimit > 0:
            l = (rateLimiterSingleton.latestRequestLimit.float * RATE_LIMITER_LIMIT_SECURITY_FACTOR).int
    var stop = false
    let start = getMonoTime()
    let timeoutDuration = initDuration(seconds = timeout)
    while not stop:
        let now = getMonoTime()
        if now - self.tick > oneMinute:
            self.weight = 0
            self.tick = now
        
        stop = self.weight < l
        if timeout != -1 and now - start > timeoutDuration:
            raise Exception.newException("Timeout")
        if not stop:
            await sleepAsync(RATE_LIMITER_SLEEP_TICK)

proc getJson(client: BinanceHttpClient, url: string, retry = 5, weight = 1, rawIntegers = false, rawFloats = false, timeout = 30_000): Future[JsonNode] {.async.} =
    proc `do`(): Future[JsonNode] {.async.} =
        for i in 0..<retry:
            var rl = rateLimiter()
            await rl.waitForLimit()
            # TODO here we need a async lock or semaphore reserving the weight
            # until the below async request is done
            let r: AsyncResponse = await client.client.get(url)
            assert weight >= 0
            rl.weight += weight
            try:
                if r.code == Http429:
                    await sleepAsync(60 * 1000)
                elif r.code == Http418:
                    await sleepAsync(3 * 60 * 1000)
                if r.code.is4xx or r.code.is5xx:
                    raise newException(HttpRequestError, r.status)
                let raw = await r.bodyStream.readAll()
                let data = parseJson(raw, rawIntegers, rawFloats)
                return data
            except:
                if i == retry - 1:
                    raise
                if i > 1:
                    let e = getCurrentException()
                    if e of ref ProtocolError or e of ProtocolError:
                        client.client = createAsyncHttpClient() # reset client https://github.com/nim-lang/Nim/issues/7316
                await sleepAsync(50)
                
    var f = `do`()
    if await withTimeout(f, timeout):
        return f.read()
    else:
        client.finalizer()
    return parseJson("{}")

func encodeQuery(query: Table[string, string], usePlus = true, omitEq = true): string =
  for key, val in pairs(query):
    # Encode the `key = value` pairs and separate them with a '&'
    if result.len > 0: result.add('&')
    result.add(encodeUrl(key, usePlus))
    # Omit the '=' if the value string is empty
    if not omitEq or val.len > 0:
      result.add('=')
      result.add(encodeUrl(val, usePlus))


proc updateRequestLimit(s: var BinanceRateLimiterSingleton, exchangeInfo: JsonNode): int =
    let rateLimits = exchangeInfo{"rateLimits"}
    var found = false
    if rateLimits != nil and rateLimits.kind == JArray:
        for rl in rateLimits:
            if rl{"rateLimitType"}.getStr() == "REQUEST_WEIGHT":
                assert rl{"interval"}.getStr() == "MINUTE"
                let intervalNum = rl{"intervalNum"}
                assert (intervalNum.kind == JInt and intervalNum.getInt() == 1) or (intervalNum.kind == JString and intervalNum.getStr() == "1")
                let limit = rl{"limit"}
                var limitInt: int = -1
                if limit.kind == JInt:
                    limitInt = limit.getInt()
                elif limit.kind == JString:
                    if parseInt(limit.getStr(), limitInt) == 0:
                        raise Exception.newException("unable to parse limit")
                else:
                    raise Exception.newException("bad limit kind")
                found = limitInt > 0
                if found:
                    result = limitInt
    if not found:
        raise Exception.newException("unable to find REQUEST_WEIGHT")
    s.latestRequestLimit = result
                

proc exchangeInfo*(self: BinanceHttpClient, rawIntegers = false, rawFloats = false): Future[JsonNode] {.async.} =
    let uri = parseUri(BASE_URL) / "fapi/v1/exchangeInfo"
    result = await self.getJson($uri, weight = 1, rawIntegers = rawIntegers, rawFloats = rawFloats)
    discard updateRequestLimit(rateLimiterSingleton, result)


proc ping*(self: BinanceHttpClient): Future[bool] {.async.} =
    let uri = parseUri(BASE_URL) / "fapi/v1/ping"
    result = len(await self.getJson($uri, weight = 1, rawIntegers = false, rawFloats = false)) == 0

proc serverTime*(self: BinanceHttpClient): Future[int64] {.async.} =
    let uri = parseUri(BASE_URL) / "fapi/v1/time"
    let j = await self.getJson($uri, weight = 1, rawIntegers = true, rawFloats = true)
    let serverTime = j{"serverTime"}.getStr()
    if parseBiggestInt(serverTime, result) == 0:
        raise Exception.newException("unable to parse serverTime")


proc getHistoricalEntries*[T](self: BinanceHttpClient, url: string, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[T]] {.async.} =
    var uri = parseUri(BASE_URL) / url
    var query = {"symbol": symbol, "period": period}.toTable
    if limit != -1:
        query["limit"] = $limit
    if startTime != -1:
        query["startTime"] = $startTime
    if endTime != -1:
        query["endTime"] = $endTime
    uri.query = encodeQuery(query)
    let j = await self.getJson($uri, rawIntegers=true, rawFloats=true, weight = 1)
    if j.kind == JObject and len(j) == 0:
        raise HttpRequestError.newException("invalid json content")
    return j.to(seq[T])

proc openInterestHist*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[OpenInterestHist]] {.async.} =
    result = await getHistoricalEntries[OpenInterestHist](self, "futures/data/openInterestHist", symbol = symbol, period = period, limit = limit, startTime = startTime, endTime = endTime)

proc topTraderLongShortRatioAccounts*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[TopTraderLongShortRatioAccounts]] {.async.} =
    result = await getHistoricalEntries[TopTraderLongShortRatioAccounts](self, "futures/data/topLongShortAccountRatio", symbol = symbol, period = period, limit = limit, startTime = startTime, endTime = endTime)

proc topTraderLongShortRatioPositions*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[TopTraderLongShortRatioPositions]] {.async.} =
    result = await getHistoricalEntries[TopTraderLongShortRatioPositions](self, "futures/data/topLongShortPositionRatio", symbol = symbol, period = period, limit = limit, startTime = startTime, endTime = endTime)
    
proc longShortRatio*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[LongShortRatio]] {.async.} =
    result = await getHistoricalEntries[LongShortRatio](self, "/futures/data/globalLongShortAccountRatio", symbol = symbol, period = period, limit = limit, startTime = startTime, endTime = endTime)