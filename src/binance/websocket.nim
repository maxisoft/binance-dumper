import std/times
import std/monotimes
import asyncdispatch
import ws
import ./cancellationtoken

type FutureBinanceWebSocket* = ref object
    ws: WebSocket
    url: string
    connect_counter: int64
    message_counter: int64
    ping_time: MonoTime
    callback*: proc (data: string): bool


proc newFutureBinanceWebSocket*(url: string): FutureBinanceWebSocket =
    result.new()
    result.url = url
    result.ping_time = getMonoTime()

proc connect*(self: FutureBinanceWebSocket) {.async.} =
    if self.ws == nil or self.ws.readyState != ReadyState.Open:
        self.ws = await newWebSocket(self.url)
        self.ping_time = getMonoTime()
        inc self.connect_counter

proc loop*(self: FutureBinanceWebSocket, cancellationToken: CancellationToken) {.async.} =
    while not cancellationToken.cancelled:
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
        inc self.message_counter
        if getMonoTime() - t < initDuration(milliseconds = 50) and not cancellationToken.cancelled:
            await sleepAsync(50)

    if self.ws != nil:
        self.ws.hangup()

