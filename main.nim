import std/strformat

import os
import std/json
import std/enumerate
import std/monotimes
import std/sequtils
import binance_data
import binance_ws
import csv_writer
import asyncdispatch
import state
import binance_http
import client_sched
import pair_tracker


let startMonoTime = getMonoTime()

proc loadPairTracker(): PairTracker =
    result = newPairTracker()
    if fileExists("pairs.json"):
        let f = open("pairs.json")
        defer:
            f.close()
        let content = f.readAll()
        result.importPairJson(parseJson(content))

proc createFreshPairTracker(stateLoader: StateLoader): Future[PairTracker] {.async.} =
    result = newPairTracker()
    let job = newUpdatePairTrackerJob(stateLoader, result, getMonoTime())
    await job.updatePairs()

proc main() =
    let pairTrackerInstance = loadPairTracker()
    let stateLoader = newStateLoader("state.json")
    var st: State = stateLoader.get()
    try:
        waitFor stateLoader.load()
    except OSError:
        discard
    defer:
        stateLoader.close()
    st = stateLoader.get
    st.incrementVersion()
    echo st.version
    let freshPairTracker = waitFor createFreshPairTracker(stateLoader)
    let sched = newJobScheduler(stateLoader)
    var pairs = newSeq[string]()
    for p in freshPairTracker.listPair():
        pairs.add(p)
    let periods = ["5m", "1h", "1d"]
    let client = newBinanceHttpClient()
    for i, pair in enumerate(pairs):
        if not dirExists(pair):
            createDir(pair)
        if not dirExists(pair / "openinterest"):
            createDir(pair / "openinterest")
        if not dirExists(pair / "topTraderLongShortRatioAccounts"):
            createDir(pair / "topTraderLongShortRatioAccounts")
        if not dirExists(pair / "topTraderLongShortRatioPositions"):
            createDir(pair / "topTraderLongShortRatioPositions")
        if not dirExists(pair / "longShortRatio"):
            createDir(pair / "longShortRatio")
        for period in periods:
            block:
                let csvOI = newCsvWritter($pair / "openinterest" / $period, timeUnit=CsvTimeUnit.monthly)
                let jobOi = newOpenInterestHistJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvOI, client = client, dueTime = startMonoTime)
                sched.add(jobOi)
            block:
                let csvtLSAR = newCsvWritter($pair / "topTraderLongShortRatioAccounts" / $period, timeUnit=CsvTimeUnit.monthly)
                let jobLSAR = newTopTraderLongShortRatioAccountsJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvtLSAR, client = client, dueTime = startMonoTime)
                sched.add(jobLSAR)
            block:
                let csvtLSPR = newCsvWritter($pair / "topTraderLongShortRatioPositions" / $period, timeUnit=CsvTimeUnit.monthly)
                let jobLSPR = newTopTraderLongShortRatioPositionsJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvtLSPR, client = client, dueTime = startMonoTime)
                sched.add(jobLSPR)
            block:
                let csvLSR = newCsvWritter($pair / "longShortRatio" / $period, timeUnit=CsvTimeUnit.monthly)
                let jobLSR = newLongShortRatioJob(symbol = pair, period = period, startTime = -1, stateLoader = stateLoader, csvWritter = csvLSR, client = client, dueTime = startMonoTime)
                sched.add(jobLSR)

    sched.add(newUpdatePairTrackerJob(stateLoader, pairTrackerInstance, getMonoTime()))

    asyncCheck stateLoader.save()
    let b = newFutureBinanceWebSocket("wss://fstream.binance.com/ws/btcusdt@depth20")
    let csv_w = newCsvWritter("btcusdt_depth20")
    b.callback = csv_w.makeCallback(bookStreamToCsv)
    waitFor b.connect()
    let bmp = newFutureBinanceWebSocket("wss://fstream.binance.com/ws/!markPrice@arr")
    let csv_mark = newCsvWritter("markprice")
    bmp.callback = csv_mark.makeCallback(markPriceToCsv, iterateArray = true)
    waitFor bmp.connect()
    asyncCheck b.loop()
    asyncCheck bmp.loop()
    asyncCheck csv_mark.loop()
    asyncCheck sched.loop()
    let task = csv_w.loop()
    when defined(useRealtimeGC):
        asyncCheck task
        while true:
            GC_step(50)
            sleep(100)
    else:
        waitFor task

when isMainModule:
    main()