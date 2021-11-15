import asyncdispatch
import std/json
import std/strformat
import std/math
import std/times
import std/deques
import std/asyncfile

type 
    CsvTimeUnit* = enum
        hourly, daily, monthly

    CsvWritter* = ref object
        identifier: string
        processingQueue: Deque[(string, Time)]
        processingThreshold*: int
        timeUnit: CsvTimeUnit

proc newCsvWritter*(identifer: string, timeUnit: CsvTimeUnit = CsvTimeUnit.hourly, processingThreshold = 600): CsvWritter =
    assert processingThreshold > 0
    result.new()
    result.identifier = identifer
    result.processingQueue = initDeque[(string, Time)]()
    result.processingThreshold = processingThreshold
    result.timeUnit = timeUnit

const 
    hourly_format = "yyyy-MM-dd'_'HH" 
    daily_format = "yyyy-MM-dd"
    monthly_format = "yyyy-MM"

proc timeFormat*(self: CsvWritter): string {.inline.} = 
    return (case self.timeUnit
        of CsvTimeUnit.hourly: hourly_format
        of CsvTimeUnit.daily: daily_format
        of CsvTimeUnit.monthly: monthly_format)


proc append*(self: CsvWritter, value: string, date: Time) {.inline.} = 
    self.processingQueue.addLast((value, date))

proc append*(self: CsvWritter, value: string, date: DateTime) {.inline.} = append(self, value, date.toTime)

proc append*(self: CsvWritter, value: string) {.inline.} = append(self, value, now().utc)

proc getFileName*(self: CsvWritter, dt: DateTime | Time): string =
    let d = dt.format(self.timeFormat)
    return fmt"{self.identifier}_{d}.csv"

proc makeCallback*(self: CsvWritter, transformer: proc(data: JsonNode): string, iterateArray = false): proc(data: string): bool =
    result = proc (data: string): bool =
                try:
                    let j = parseJson(data)
                    #echo j
                    let n = now()
                    if iterateArray and j.kind == JArray:
                        for e in j:
                            self.append(transformer(e), n)
                    else:
                        self.append(transformer(j), n)
                    return false
                except JsonParsingError:
                    # TODO count error and throw if too much
                    return false

proc makeCallback*(self: CsvWritter, transformer: proc(data: JsonNode, date: var Time): string, iterateArray = false): proc(data: string): bool =
    result = proc (data: string): bool =
                try:
                    let j = parseJson(data)
                    #echo j
                    if iterateArray and j.kind == JArray:
                        for e in j:
                            var date: Time
                            let transformed = transformer(e, date)
                            self.append(transformed, date)
                    else:
                        var date: Time
                        let transformed = transformer(j, date)
                        self.append(transformed, date)
                    return false
                except JsonParsingError:
                    # TODO count error and throw if too much
                    return false


proc drainProcessingQueue(self: CsvWritter, threshold: int) {.async.} =
    assert threshold >= 0
    if len(self.processingQueue) > max(threshold, 0):
        let (_, first_date) = self.processingQueue.peekFirst()
        var fileName = getFileName(self, first_date)
        var file = openAsync(fileName, fmAppend)
        let timeFormat = self.timeFormat
        try:
            var prev_time = first_date.format(timeFormat)
            while len(self.processingQueue) > 0:
                let (data, date) = self.processingQueue.popFirst()
                try:
                    let current_time = date.format(timeFormat)
                    if current_time != prev_time:
                        file.close()
                        fileName = getFileName(self, date)
                        file = openAsync(fileName, fmAppend)
                        prev_time = current_time
                    await file.write(data)
                    await file.write("\n")
                except:
                    self.processingQueue.addFirst((data, date))
                    raise
        finally:
            file.close()

proc drainProcessingQueue(self: CsvWritter) {.async.} = await drainProcessingQueue(self, self.processingThreshold)

proc processNow*(self: CsvWritter) {.async.} = await drainProcessingQueue(self, 0)

proc loop*(self: CsvWritter) {.async.} =
    while true:
        await drainProcessingQueue(self)
        await sleepAsync(1000)

func identifier*(self: var CsvWritter): string {.inline.} =
    return self.identifier