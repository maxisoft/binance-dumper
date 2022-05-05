import std/strformat

import os
import std/json
import std/enumerate
import std/monotimes
import std/sequtils
import std/times
import std/tables
import std/parseutils
import binance/cancellationtoken
import binance/data
import binance/websocket
import binance/csvwriter
import asyncdispatch
import binance/state
import binance/http
import binance/scheduler
import binance/pairtracker
import binance/httppool


let startMonoTime = getMonoTime()

proc loadPairTracker(): PairTracker =
    result = newPairTracker()
    if fileExists("pairs.json"):
        let f = open("pairs.json")
        defer:
            f.close()
        let content = f.readAll()
        result.importPairJson(parseJson(content))

proc createFreshPairTracker(stateLoader: StateLoader, pool: HttpPool): Future[PairTracker] {.async.} =
    result = newPairTracker()
    let job = newUpdatePairTrackerJob(stateLoader, result, newBinanceHttpClient(pool), getMonoTime())
    await job.updatePairs()

proc main() =
    var cancellationToken = newCancellationToken()
    defer:
        cancellationToken.cancel(throws = false)
    let pairTrackerInstance = loadPairTracker()
    let stateLoader = newStateLoader("state.json")
    let pool = newHttpPool()
    let freshPairTracker = waitFor createFreshPairTracker(stateLoader, pool)
    let sched = newJobScheduler(stateLoader)
    if existsEnv("SCHEDULER_CONCURRENT_TASK"):
        var maxTasks: int64
        let v = getEnv("SCHEDULER_CONCURRENT_TASK")
        if parseBiggestInt(v, maxTasks) == 0:
            raise Exception.newException(fmt"invalid SCHEDULER_CONCURRENT_TASK = {v}")
        sched.maxTasks = maxTasks
        if maxTasks > 0:
            pool.max_connections = min(pool.max_connections, maxTasks.uint)

    if existsEnv("BINANCE_HTTP_POOL_CONNECTION"):
        var max_connections = 0
        let v = getEnv("BINANCE_HTTP_POOL_CONNECTION")
        if parseInt(v, max_connections) == 0 or max_connections <= 0:
            raise Exception.newException(fmt"invalid BINANCE_HTTP_POOL_CONNECTION = {v}")
        pool.max_connections = max_connections.uint
    
    var pairs = newSeq[string]()
    for p in freshPairTracker.listPair():
        pairs.add(p)
    let periods = ["5m", "1h", "1d"]
    let client = newBinanceHttpClient(pool)
    for i, pair in enumerate(pairs):
        if not dirExists(pair):
            createDir(pair)
        if not dirExists(pair / openinterestName):
            createDir(pair / openinterestName)
        if not dirExists(pair / topTraderLongShortRatioAccountsName):
            createDir(pair / topTraderLongShortRatioAccountsName)
        if not dirExists(pair / topTraderLongShortRatioPositionsName):
            createDir(pair / topTraderLongShortRatioPositionsName)
        if not dirExists(pair / longShortRatioName):
            createDir(pair / longShortRatioName)
        if not dirExists(pair / takerBuySellRatioName):
            createDir(pair / takerBuySellRatioName)
        for period in periods:
            block:
                var csvOI = newCsvWritter($pair / openinterestName / $period, timeUnit=CsvTimeUnit.monthly)
                let jobOi = newOpenInterestHistJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvOI, client = client, dueTime = startMonoTime)
                csvOI.identifier = $pair / jobOi.name() / $period
                sched.add(jobOi)
            block:
                var csvtLSAR = newCsvWritter($pair / topTraderLongShortRatioAccountsName / $period, timeUnit=CsvTimeUnit.monthly)
                let jobLSAR = newTopTraderLongShortRatioAccountsJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvtLSAR, client = client, dueTime = startMonoTime)
                csvtLSAR.identifier = $pair / jobLSAR.name() / $period
                sched.add(jobLSAR)
            block:
                var csvtLSPR = newCsvWritter($pair / topTraderLongShortRatioPositionsName / $period, timeUnit=CsvTimeUnit.monthly)
                let jobLSPR = newTopTraderLongShortRatioPositionsJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvtLSPR, client = client, dueTime = startMonoTime)
                csvtLSPR.identifier = $pair / jobLSPR.name() / $period
                sched.add(jobLSPR)
            block:
                var csvLSR = newCsvWritter($pair / longShortRatioName / $period, timeUnit=CsvTimeUnit.monthly)
                let jobLSR = newLongShortRatioJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvLSR, client = client, dueTime = startMonoTime)
                csvLSR.identifier = $pair / jobLSR.name() / $period
                sched.add(jobLSR)
            block:
                var csTBSV = newCsvWritter($pair / takerBuySellRatioName / $period, timeUnit=CsvTimeUnit.monthly)
                let jobTBSV = newTakerBuySellVolumeJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csTBSV, client = client, dueTime = startMonoTime)
                csTBSV.identifier = $pair / jobTBSV.name() / $period
                sched.add(jobTBSV)

    sched.add(newUpdatePairTrackerJob(stateLoader, pairTrackerInstance, client, getMonoTime()))

    if len(getEnv("BTC_WEBSOCKET", "")) > 0:
        # BTC_WEBSOCKET => write down btc depth and markprice
        # Generate large files overtime
        echo "Processing btc websocket too..."
        let b = newFutureBinanceWebSocket("wss://fstream.binance.com/ws/btcusdt@depth20")
        let csv_w = newCsvWritter("btcusdt_depth20")
        b.callback = csv_w.makeCallback(bookStreamToCsv)
        waitFor b.connect()
        let bmp = newFutureBinanceWebSocket("wss://fstream.binance.com/ws/!markPrice@arr")
        let csv_mark = newCsvWritter("markprice")
        bmp.callback = csv_mark.makeCallback(markPriceToCsv, iterateArray = true)
        waitFor bmp.connect()
        asyncCheck b.loop(cancellationToken)
        asyncCheck bmp.loop(cancellationToken)
        asyncCheck csv_mark.loop(cancellationToken)
        asyncCheck csv_w.loop(cancellationToken)

    asyncCheck pool.loop(cancellationToken)
    let task = sched.loop(cancellationToken)
    waitFor task

when isMainModule:
    main()