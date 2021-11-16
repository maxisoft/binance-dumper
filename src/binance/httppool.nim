import asyncdispatch
import std/times
import std/asyncnet
import std/monotimes
import nativesockets
import std/httpclient
import std/deques
import std/os
import std/lists
import ./cancellationtoken

const 
    USER_AGENT = "nim/1.0"
    MAX_POOL_CONNECTION = 20
    FRESHLY_CREATED_LIMIT_PER_MINUTE = 20
    oneMinute = initDuration(minutes = 1)
    oneHour = initDuration(hours = 1)

type 
    PoolEntry = object
        client: AsyncHttpClient
        useCounter: int64
        creationDate: MonoTime
        lastUseDate: MonoTime

    HttpPool* = ref object
        pool: DoublyLinkedList[PoolEntry]
        awaiters: Deque[Future[void]]

proc createAsyncHttpClient(): AsyncHttpClient =
    let ua = getEnv("USER_AGENT", USER_AGENT)
    result = newAsyncHttpClient(userAgent=ua)

iterator clients*(self: HttpPool): var AsyncHttpClient =
    for item in mitems(self.pool):
        yield item.client

iterator entries*(self: HttpPool): var PoolEntry =
    for item in mitems(self.pool):
        yield item

proc rent*(self: HttpPool, useCount = 1, throwOnConnectLimit=false): AsyncHttpClient =
    var freshlyCreatedCounter = 0
    let now = getMonoTime()
    var c: int64 = 0
    for e in entries(self):
        if abs(now - e.creationDate) < oneHour and e.useCounter == 0:
            inc e.useCounter, useCount
            e.lastUseDate = getMonoTime()
            return e.client
        if abs(now - e.creationDate) < oneMinute:
            inc freshlyCreatedCounter
        inc c

    if freshlyCreatedCounter >= FRESHLY_CREATED_LIMIT_PER_MINUTE or c >= MAX_POOL_CONNECTION:
        if throwOnConnectLimit:
            raise Exception.newException("throwOnConnectLimit")
        assert not self.pool.head.isNil
        var best = self.pool.tail
        for n in nodes(self.pool):
            if n.value.useCounter < best.value.useCounter and abs(now - n.value.creationDate) < oneHour:
                best = n
        inc best.value.useCounter, useCount
        return best.value.client
    
    self.pool.add(PoolEntry(client: createAsyncHttpClient(), creationDate: now, lastUseDate: now))
    inc self.pool.tail.value.useCounter, useCount
    return self.pool.tail.value.client

proc rentAsync*(self: HttpPool): Future[AsyncHttpClient] {.async.}=
    while true:
        try:
            return self.rent(1, throwOnConnectLimit = true)
        except:
            if getCurrentExceptionMsg() == "throwOnConnectLimit":
                var w = newFuture[void]("HttpPool.rentAsync")
                self.awaiters.addLast(w)
                yield w
            else:
                raise

proc notify(self: HttpPool) =
    if len(self.awaiters) == 0:
        return
    var waiters = newSeqOfCap[Future[void]](1)
    while len(self.awaiters) > 0:
        let w = self.awaiters.popFirst()
        if not w.finished:
            waiters.add(w)
            break

    proc completeAll() =
        for f in waiters:
            if not f.finished:
                f.complete()

    callSoon(completeAll)

proc isClosed(self: AsyncHttpClient, nilCountsTrue = false): bool =
    let s = self.getSocket()
    if s.isNil: return nilCountsTrue
    result = s.isClosed

proc shouldClose(n: DoublyLinkedNode[PoolEntry]): bool {.inline.} =
    result = false
    let now = getMonoTime()
    if n.value.useCounter <= 0 and n.value.client.isClosed() and abs(now - n.value.creationDate) > oneMinute:
        return true
    if abs(now - n.value.lastUseDate) > 2 * oneHour: # no connections should last 2hr
        return true

proc `return`*(self: HttpPool, client: AsyncHttpClient, useCount = 1) =
    for n in nodes(self.pool):
        if cast[pointer](n.value.client) == cast[pointer](client):
            dec n.value.useCounter, useCount
            n.value.useCounter = max(n.value.useCounter, 0)
            if shouldClose(n):
                client.close()
                self.pool.remove(n)
            notify(self)
            return
    raise Exception.newException("Trying to return a not managed client")

proc cleanup*(self: HttpPool) =
    var stable = false
    while not stable:
        stable = true
        for n in nodes(self.pool):
            if shouldClose(n):
                let client = n.value.client
                if client.isNil:
                    continue
                client.close()
                self.pool.remove(n)
                stable = false
                notify(self)
                break

proc loop*(self: HttpPool, token: CancellationToken, sleepTime = 60_000) {.async.} =
    while not token.cancelled:
        await sleepAsync(sleepTime)
        cleanup(self)

template withClient*(self: HttpPool, code: untyped) =
    let client {.inject.} = self.rent()
    try:
        code
    finally:
        self.`return`(client)

template withClient*(self: HttpPool, useCount: int, code: untyped) =
    let client {.inject.} = self.rent(useCount=useCount)
    try:
        code
    finally:
        self.`return`(client, useCount=useCount)


