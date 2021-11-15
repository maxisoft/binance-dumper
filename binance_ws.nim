import asyncdispatch
import ws
import std/sets
import std/json
import std/strformat
import std/math
import std/times
import std/monotimes

type FutureBinanceWebSocket* = ref object
    ws: WebSocket
    url: string
    connect_counter: int64
    message_counter: int64
    ping_time: MonoTime
    request_stop*: bool
    callback*: proc (data: string): bool


proc newFutureBinanceWebSocket*(url: string): FutureBinanceWebSocket =
    result.new()
    result.url = url
    result.ping_time = getMonoTime()

proc connect*(self: FutureBinanceWebSocket) {.async.} =
    if self.ws == nil or self.ws.readyState != ReadyState.Open:
        self.ws = await newWebSocket(self.url)
        self.ping_time = getMonoTime()
        self.connect_counter += 1


proc loop*(self: FutureBinanceWebSocket) {.async.} =
    while not self.request_stop:
        let t = getMonoTime()
        if self.ws == nil or self.ws.readyState != ReadyState.Open:
            await connect(self)
            await sleepAsync(150)
            continue
        if t - self.ping_time > initDuration(seconds = 60):
            await self.ws.ping()
            self.ping_time = getMonoTime()
        let data = await self.ws.receiveStrPacket()
        if self.callback != nil:
            if self.callback(data):
                break
        else:
            echo data
        self.message_counter += 1
        if getMonoTime() - t < initDuration(milliseconds = 50) and not self.request_stop:
            await sleepAsync(50)

    if self.ws != nil:
        self.ws.close()

