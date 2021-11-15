import asyncdispatch
import std/json
import std/times
import std/monotimes
import std/httpclient
import std/tables
import std/strformat
import std/strutils
import std/parseutils
import std/heapqueue
import std/asyncfile
import std/locks
import ./data
import ./http
import ./csvwriter
import ./state
import ./pairtracker


type 
    BaseJob {.inheritable.} = ref object of RootObj
        dueTime: MonoTime
        stateLoader: StateLoader

    CsvWriterJob {.inheritable.} = ref object of BaseJob
        csvWritter: CsvWritter

    BinanceHttpJob {.inheritable.} = ref object of CsvWriterJob
        client: BinanceHttpClient
        clientOwned: BinanceHttpClient
        clientLock: Lock

    BaseBinanceHistorycalEntryJob = ref object of BinanceHttpJob
        symbol: string
        period : string
        startTime: int64
        parsedPeriod: Duration

    OpenInterestHistJob* = ref object of BaseBinanceHistorycalEntryJob
    TopTraderLongShortRatioAccountsJob* = ref object of BaseBinanceHistorycalEntryJob
    TopTraderLongShortRatioPositionsJob* = ref object of BaseBinanceHistorycalEntryJob
    LongShortRatioJob* = ref object of BaseBinanceHistorycalEntryJob

    UpdatePairTrackerJob* = ref object of BaseJob
        tracker: PairTracker

    
proc state(self: BaseJob): var State {.inline.} = result = self.stateLoader.get

proc parsePeriod*(period: string): Duration =
    if len(period) < 2:
        raise Exception.newException("not a valid period")
    let last: char = period[^1]
    let tmp = period[0..^2]
    assert len(tmp) == len(period) - 1
    var value: int64
    if parseBiggestInt(tmp, value) == 0:
        raise Exception.newException("not a valid period")
    result = (case last
        of 's': initDuration(seconds = value)
        of 'm': initDuration(minutes = value)
        of 'h': initDuration(hours = value)
        of 'd': initDuration(days = value)
        of 'w': initDuration(weeks = value)
        of 'M': initDuration(days = 30 * value)
        else: raise Exception.newException("not a valid period")
    )
        
proc finalizer*(job: BinanceHttpJob) =
    if job.clientOwned != nil:
        job.clientOwned.finalizer()
    deinitLock(job.clientLock)

proc newBaseBinanceHistorycalEntryJob[T](symbol: string, period: string, startTime: int64, stateLoader: StateLoader, csvWritter: CsvWritter, client: BinanceHttpClient, dueTime: MonoTime, clientOwned = false): T = 
    when defined(gcOrc) or defined(gcArc):
        result.new()
    else:
        result.new(proc (x: T) = x.finalizer())
    initLock(result.clientLock)
    result.symbol = symbol
    result.period = period
    result.startTime = startTime
    result.stateLoader = stateLoader
    result.csvWritter = csvWritter
    result.client = client
    result.dueTime = dueTime
    result.parsedPeriod = parsePeriod(period)
    if clientOwned:
        result.clientOwned = client

template newBBHEJob(x: untyped): untyped =
    newBaseBinanceHistorycalEntryJob[x](symbol = symbol, period = period, startTime = startTime, stateLoader = stateLoader, csvWritter = csvWritter, client = client, dueTime = dueTime)

proc newOpenInterestHistJob*(symbol: string, period: string, startTime: int64, stateLoader: StateLoader, csvWritter: CsvWritter, client: BinanceHttpClient, dueTime: MonoTime): OpenInterestHistJob = 
    result = newBBHEJob(OpenInterestHistJob)

proc newTopTraderLongShortRatioAccountsJob*(symbol: string, period: string, startTime: int64, stateLoader: StateLoader, csvWritter: CsvWritter, client: BinanceHttpClient, dueTime: MonoTime): TopTraderLongShortRatioAccountsJob = 
    result = newBaseBinanceHistorycalEntryJob[TopTraderLongShortRatioAccountsJob](symbol = symbol, period = period, startTime = startTime, stateLoader = stateLoader, csvWritter = csvWritter, client = client, dueTime = dueTime)

proc newTopTraderLongShortRatioPositionsJob*(symbol: string, period: string, startTime: int64, stateLoader: StateLoader, csvWritter: CsvWritter, client: BinanceHttpClient, dueTime: MonoTime): TopTraderLongShortRatioPositionsJob = 
    result = newBaseBinanceHistorycalEntryJob[TopTraderLongShortRatioPositionsJob](symbol = symbol, period = period, startTime = startTime, stateLoader = stateLoader, csvWritter = csvWritter, client = client, dueTime = dueTime)

proc newLongShortRatioJob*(symbol: string, period: string, startTime: int64, stateLoader: StateLoader, csvWritter: CsvWritter, client: BinanceHttpClient, dueTime: MonoTime): LongShortRatioJob = 
    result = newBaseBinanceHistorycalEntryJob[LongShortRatioJob](symbol = symbol, period = period, startTime = startTime, stateLoader = stateLoader, csvWritter = csvWritter, client = client, dueTime = dueTime)

proc `<`(a, b: BaseJob): bool = a.dueTime < b.dueTime

method invoke(this: BaseJob) {.base async.} =
    raise Exception.newException("must be implemented")

method incrementDueTime(this: BaseJob) {.base.} =
    raise Exception.newException("must be implemented")

method savedTimeStampTable(this: BaseBinanceHistorycalEntryJob): var Table[string, SavedTimestampState] {.base.} =
    result = this.state.openInterestHist
    raise Exception.newException("must be implemented")

method listEntries(this: BaseBinanceHistorycalEntryJob, startTime: int64, limit: int): Future[seq[BaseBinanceHistorycalEntry]] {.base async.} =
    raise Exception.newException("must be implemented")

method defaultStartTime(this: BaseBinanceHistorycalEntryJob): int64 {.base.} =
    return (now().utc.toTime - initDuration(days=29)).toUnix * 1000

method defaultLimit(this: BaseBinanceHistorycalEntryJob): int {.base.} =
    return 400 # max 500 but better safe than sorry

method incrementDueTime(this: BaseBinanceHistorycalEntryJob) =
    this.dueTime = max(this.dueTime, getMonoTime()) + min(this.parsedPeriod, initDuration(hours=1))

proc doListEntries[T](self: BaseBinanceHistorycalEntryJob, startTime: int64, limit: int, getter: proc (client: BinanceHttpClient, startTime: int64, limit: int): Future[seq[T]]): Future[seq[BaseBinanceHistorycalEntry]] {.async.} =
    var tmp: seq[T]
    try:
        tmp = await getter(self.client, startTime = startTime, limit = limit)
    except ProtocolError:
        withLock(self.clientLock):
            if self.clientOwned != nil:
                self.clientOwned.finalizer()
            self.client = newBinanceHttpClient()
            self.clientOwned = self.client
        tmp = await getter(self.client, startTime = startTime, limit = limit)
    result = newSeqOfCap[BaseBinanceHistorycalEntry](len(tmp))
    for item in tmp:
        result.add(item)

proc computeEndTime(startTime: int64, limit: int, period: Duration): int64 =
    result = -1
    if startTime != -1:
        assert limit > 0
        result = startTime + period.inSeconds * 1000 * limit

method savedTimeStampTable(this: OpenInterestHistJob): var Table[string, SavedTimestampState] = result = this.state.openInterestHist

method listEntries(this: OpenInterestHistJob, startTime: int64, limit: int): Future[seq[BaseBinanceHistorycalEntry]] {.async.} =
    proc getter (client: BinanceHttpClient, startTime: int64, limit: int): Future[seq[OpenInterestHist]] {.async.} =
        let endTime = computeEndTime(startTime, limit, this.parsedPeriod)
        result = await client.openInterestHist(symbol = this.symbol, period = this.period, startTime = startTime, endTime = endTime, limit = limit)
    
    result = await doListEntries(this, startTime = startTime, limit = limit, getter = getter)


method savedTimeStampTable(this: TopTraderLongShortRatioAccountsJob): var Table[string, SavedTimestampState] = result = this.state.topLongShortAccountRatio

method listEntries(this: TopTraderLongShortRatioAccountsJob, startTime: int64, limit: int): Future[seq[BaseBinanceHistorycalEntry]] {.async.} =
    proc getter (client: BinanceHttpClient, startTime: int64, limit: int): Future[seq[TopTraderLongShortRatioAccounts]] {.async.} =
        let endTime = computeEndTime(startTime, limit, this.parsedPeriod)
        result = await client.topTraderLongShortRatioAccounts(symbol = this.symbol, period = this.period, startTime = startTime, endTime = endTime, limit = limit)
    
    result = await doListEntries(this, startTime = startTime, limit = limit, getter = getter)

method savedTimeStampTable(this: TopTraderLongShortRatioPositionsJob): var Table[string, SavedTimestampState] = result = this.state.topLongShortPositionRatio

method listEntries(this: TopTraderLongShortRatioPositionsJob, startTime: int64, limit: int): Future[seq[BaseBinanceHistorycalEntry]] {.async.} =
    proc getter (client: BinanceHttpClient, startTime: int64, limit: int): Future[seq[TopTraderLongShortRatioPositions]] {.async.} =
        let endTime = computeEndTime(startTime, limit, this.parsedPeriod)
        result = await client.topTraderLongShortRatioPositions(symbol = this.symbol, period = this.period, startTime = startTime, endTime = endTime, limit = limit)
    
    result = await doListEntries(this, startTime = startTime, limit = limit, getter = getter)

method savedTimeStampTable(this: LongShortRatioJob): var Table[string, SavedTimestampState] = result = this.state.globalLongShortAccountRatio

method listEntries(this: LongShortRatioJob, startTime: int64, limit: int): Future[seq[BaseBinanceHistorycalEntry]] {.async.} =
    proc getter (client: BinanceHttpClient, startTime: int64, limit: int): Future[seq[LongShortRatio]] {.async.} =
        let endTime = computeEndTime(startTime, limit, this.parsedPeriod)
        result = await client.longShortRatio(symbol = this.symbol, period = this.period, startTime = startTime, endTime = endTime, limit = limit)
    
    result = await doListEntries(this, startTime = startTime, limit = limit, getter = getter)

method invoke(this: BaseBinanceHistorycalEntryJob) {.async.} =
    let key = fmt"{this.symbol}_{this.period}"
    var startTime = this.startTime
    if startTime == -1: # special case for 1st time
        let stt = this.savedTimeStampTable
        if key in stt and parseBiggestInt(stt[key].timestamp, startTime) != 0:
            discard
        else:
            startTime = this.defaultStartTime
    let limit = this.defaultLimit
    let history = await this.listEntries(startTime = startTime, limit = limit)
    if len(history) > 0:
        var maxTime: int64 = 0
        var tmp: int64 = 0
        for item in history:
            this.csvWritter.append(item.toCsv, item.date)
            if parseBiggestInt(item.timestamp, tmp) > 0:
                maxTime = max(maxTime, tmp)
        await this.csvWritter.processNow()
        var ts: SavedTimestampState
        ts.timestamp = $(maxTime)
        this.savedTimeStampTable[key] = ts
        this.state.incrementVersion()
        this.startTime = maxTime
        
        if limit > 2 and len(history) >= limit - 2:
            # there's still more history to download
            this.dueTime = max(this.dueTime, getMonoTime()) + initDuration(seconds = 1)

proc newUpdatePairTrackerJob*(stateLoader: StateLoader, tracker: PairTracker, dueTime: MonoTime): UpdatePairTrackerJob = 
    result.new()
    result.stateLoader = stateLoader
    result.dueTime = dueTime
    result.tracker = tracker

proc updatePairs*(self: UpdatePairTrackerJob) {.async.} =
    await self.invoke()

method invoke(this: UpdatePairTrackerJob) {.async.} =
    let exchangeInfo = await newBinanceHttpClient().exchangeInfo()
    let symbols = exchangeInfo{"symbols"}
    var updated = false
    for s in symbols:
        let pair = s{"pair"}.getStr()
        #let contractType = s{"contractType"}.getStr()
        let status = s{"status"}.getStr()
        if status == "TRADING":
            updated = this.tracker.addPair(pair.toLowerAscii) or updated
        
    if updated:
        let j = this.tracker.exportPairJson()
        var file = openAsync("pairs.json", fmWrite)
        try:
            var buff: string
            toUgly(buff, j)
            await file.write(buff)
        finally:
            file.close()

method incrementDueTime(this: UpdatePairTrackerJob) =
    this.dueTime = max(this.dueTime, getMonoTime()) + initDuration(minutes = 5)

type 
    JobScheduler* = ref object
        queue: HeapQueue[BaseJob]
        stateVersion: int64
        stateLoader: StateLoader

const SCHEDULER_MIN_SLEEP_TICK = 250

proc newJobScheduler*(loader: StateLoader): JobScheduler =
    result.new()
    result.queue = initHeapQueue[BaseJob]()
    result.stateVersion = loader.get().version
    result.stateLoader = loader

proc add*(self: JobScheduler, job: BaseJob) =
    self.queue.push(job)
    
proc loop*(self: JobScheduler) {.async.} =
    while len(self.queue) > 0:
        let now = getMonoTime()
        let first = self.queue[0]
        if first.dueTime > now:
            let sleepDuration = min(SCHEDULER_MIN_SLEEP_TICK, (first.dueTime - now).inMilliseconds)
            await sleepAsync(sleepDuration.float)
            continue
        var item = self.queue.pop()
        let dueTime = item.dueTime
        try:
            await item.invoke() 
            # TODO once https://github.com/nim-lang/Nim/issues/7316 is fixed
            # batch process more async work (eg invoke())
            if item.dueTime == dueTime:
                item.incrementDueTime()
        finally:
            self.queue.push(item)
        
        if item.state.compareVersion(self.stateVersion) > 0:
            await self.stateLoader.save()
            self.stateVersion = item.state.version
