import asyncdispatch
import std/json
import std/times
import std/monotimes
import std/httpclient
import std/uri
import std/tables
import std/parseutils
import std/strformat
import ./data
import ./httppool


type 
    BinanceRateLimiter* = ref object
        weight: int64
        tick: MonoTime

    BinanceRateLimiterSingleton = object
        rl: BinanceRateLimiter
        latestRequestLimit: int

    BinanceHttpClient* = ref object
        pool: HttpPool
    
    TimeoutError* = object of IOError

var rateLimiterSingleton = BinanceRateLimiterSingleton()

proc rateLimiter(self: var BinanceRateLimiterSingleton): var BinanceRateLimiter =
    if self.rl.isNil:
        self.rl.new()
        self.rl.tick = getMonoTime()
        self.latestRequestLimit = 0
    result = self.rl

proc rateLimiter*(): var BinanceRateLimiter =
    result = rateLimiterSingleton.rateLimiter

const 
    BASE_URL = "https://fapi.binance.com"
    RATE_LIMITER_DEFAULT_LIMIT = 600 # 2400 last time checked
    RATE_LIMITER_LIMIT_SECURITY_FACTOR = 0.9 # for now we limit the request/second even more (avoiding bans)
    RATE_LIMITER_SLEEP_TICK = 50 # ms
    oneMinute = initDuration(minutes = 1)

proc newBinanceHttpClient*(pool: HttpPool): BinanceHttpClient =
    result.new()
    result.pool = pool

proc effectiveLimit(limit = -1): int =
    result = limit
    if limit == -1:
        result = RATE_LIMITER_DEFAULT_LIMIT
        if rateLimiterSingleton.latestRequestLimit > 0:
            result = (rateLimiterSingleton.latestRequestLimit.float * RATE_LIMITER_LIMIT_SECURITY_FACTOR).int

proc limitReached(self: BinanceRateLimiter, limit = -1): bool =
    result = self.weight >= effectiveLimit(limit)

proc limitReached*(self: BinanceHttpClient, limit = -1): bool =
    result = limitReached(rateLimiter(), limit)

proc waitForLimit*(self: BinanceRateLimiter, limit = -1, timeout = -1) {.async.} =
    var stop = false
    let start = getMonoTime()
    let timeoutDuration = initDuration(seconds = timeout)
    while not stop:
        let now = getMonoTime()
        if now - self.tick > oneMinute:
            self.weight = 0
            self.tick = now
        
        stop = not limitReached(self)
        if timeout != -1 and now - start > timeoutDuration:
            raise TimeoutError.newException("Timeout")
        if not stop:
            await sleepAsync(RATE_LIMITER_SLEEP_TICK)

proc getJson(self: BinanceHttpClient, url: string, retry = 5, weight = 1, rawIntegers = false, rawFloats = false, timeout = 30_000): Future[JsonNode] {.async.} =
    var client: AsyncHttpClient = nil

    proc `do`(): Future[JsonNode] {.async.} =

        template exHandler(closeClient: auto) =
            if closeClient and client != nil:
                close(client)
            if i == retry - 1:
                raise
        
        for i in 0..<retry:
            var rl = rateLimiter()
            await rl.waitForLimit()
            # TODO here we need a async lock or semaphore reserving the weight
            # until the below async request is done
            client = await self.pool.rentAsync()
            try:
                assert weight >= 0
                rl.weight += weight
                try:
                    var r: AsyncResponse
                    var getTask = client.get(url)
                    if await withTimeout(getTask, timeout div retry):
                        r = getTask.read()
                    else:
                        raise TimeoutError.newException("Timeout")
                    if r.code == Http429:
                        await sleepAsync(60 * 1000)
                    elif r.code == Http418:
                        await sleepAsync(3 * 60 * 1000)
                    if r.code.is4xx or r.code.is5xx:
                        raise newException(HttpRequestError, r.status)
                    var raw: string
                    var readTask = r.bodyStream.readAll()
                    if await withTimeout(readTask, timeout div retry):
                        raw = readTask.read()
                    else:
                        raise TimeoutError.newException("Timeout")
                    let data = parseJson(raw, rawIntegers, rawFloats)
                    return data
                except ProtocolError:
                    exHandler(true)
                except TimeoutError:
                    exHandler(true)
                except:
                    exHandler(i >= 2)
                    await sleepAsync(50)
            finally:
                if client != nil:
                    self.pool.`return`(client)
                    client = nil
                
    var f = `do`()
    if await withTimeout(f, timeout):
        return f.read()
    else:
        if client != nil:
            client.close()
            self.pool.`return`(client)
        raise TimeoutError.newException(fmt"unable to process request in {initDuration(milliseconds = timeout)}")
    raise Exception.newException(fmt"Unable to get a valid response in {retry} tries")

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

template historicalEntries(x: untyped, path: static[string]): Future[seq[untyped]] =
    getHistoricalEntries[x](self, path, symbol = symbol, period = period, limit = limit, startTime = startTime, endTime = endTime)

proc openInterestHist*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[OpenInterestHist]] {.async.} =
    result = await historicalEntries(OpenInterestHist, "futures/data/openInterestHist")

proc topTraderLongShortRatioAccounts*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[TopTraderLongShortRatioAccounts]] {.async.} =
    result = await historicalEntries(TopTraderLongShortRatioAccounts, "futures/data/topLongShortAccountRatio")

proc topTraderLongShortRatioPositions*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[TopTraderLongShortRatioPositions]] {.async.} =
    result = await historicalEntries(TopTraderLongShortRatioPositions, "futures/data/topLongShortPositionRatio")
    
proc longShortRatio*(self: BinanceHttpClient, symbol: string, period = "5m", limit = -1, startTime: int64 = -1, endTime: int64 = -1): Future[seq[LongShortRatio]] {.async.} =
    result = await historicalEntries(LongShortRatio, "/futures/data/globalLongShortAccountRatio")