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
    DEFAULT_USER_AGENT = "binance_dumper/1.0"
    MAX_POOL_CONNECTIONS = 20
    FRESHLY_CREATED_LIMIT_PER_MINUTE = 20
    oneMinute = initDuration(minutes = 1)
    oneHour = initDuration(hours = 1)
    
    EXPECTED_CONNECTION_LIFESPAN = oneHour
    REMOVE_AFTER_NO_USE_TIME = 10 * oneMinute

type 
    PoolEntry = ref object
        client: AsyncHttpClient
        useCounter: int64
        creationDate: MonoTime
        lastUseDate: MonoTime

    HttpPool* = ref object
        pool: DoublyLinkedList[PoolEntry]
        awaiters: Deque[Future[void]]
        max_connections*: uint

    ConnectLimitError* = object of CatchableError

proc newHttpPool*(): HttpPool =
    result.new()
    result.awaiters = initDeque[Future[void]]()
    result.pool = initDoublyLinkedList[PoolEntry]()
    result.max_connections = MAX_POOL_CONNECTIONS

proc createAsyncHttpClient(): AsyncHttpClient =
    let ua = getEnv("USER_AGENT", DEFAULT_USER_AGENT)
    result = newAsyncHttpClient(userAgent=ua)

iterator clients*(self: HttpPool): var AsyncHttpClient =
    for item in mitems(self.pool):
        yield item.client

iterator entries*(self: HttpPool): var PoolEntry =
    for item in mitems(self.pool):
        yield item

proc rent*(self: HttpPool, useCount = 1, throwOnConnectLimit=true): var AsyncHttpClient =
    var freshlyCreatedCounter = 0
    let now = getMonoTime()
    var c: uint64 = 0
    for n in nodes(self.pool):
        if n.value.isNil:
            continue
        if n.value.useCounter == 0:
            inc n.value.useCounter, useCount
            n.value.lastUseDate = now
            return n.value.client
        if abs(now - n.value.creationDate) < oneMinute:
            inc freshlyCreatedCounter
        inc c

    if self.pool.head != nil and (freshlyCreatedCounter >= FRESHLY_CREATED_LIMIT_PER_MINUTE or c >= self.max_connections):
        if throwOnConnectLimit:
            raise ConnectLimitError.newException("throwOnConnectLimit")
        var best = self.pool.tail
        for n in nodes(self.pool):
            if n.value != nil and n.value.useCounter < best.value.useCounter and abs(now - n.value.creationDate) < EXPECTED_CONNECTION_LIFESPAN:
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
        except ConnectLimitError:
            var w = newFuture[void]("HttpPool.rentAsync")
            self.awaiters.addLast(w)
            yield w

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

proc shouldClose(self: HttpPool, n: DoublyLinkedNode[PoolEntry], index = -1): bool {.inline.} =
    if n.isNil or n.value.isNil:
        return true
    result = false
    let now = getMonoTime()
    if n.value.useCounter <= 0:
        if abs(now - n.value.creationDate) > EXPECTED_CONNECTION_LIFESPAN:
            return true
        if abs(now - n.value.creationDate) > oneMinute and n.value.client.isClosed(nilCountsTrue=true):
            return true
        if abs(now - n.value.lastUseDate) > REMOVE_AFTER_NO_USE_TIME:
            return true
        if index > 0 and index.uint >= self.max_connections and abs(now - n.value.lastUseDate) > oneMinute:
            return true

proc mustClose(n: DoublyLinkedNode[PoolEntry]): bool {.inline.} =
    if n.isNil or n.value.isNil:
        return true
    result = false
    let now = getMonoTime()
    if n.value.client.isClosed(nilCountsTrue=false):
        return true
    if abs(now - n.value.creationDate) > 2 * EXPECTED_CONNECTION_LIFESPAN: # ie no connections should last 2hr
        return true

proc `return`*(self: HttpPool, client: AsyncHttpClient, useCount = 1) =
    var i = 0
    for n in nodes(self.pool):
        if n.value != nil and cast[pointer](n.value.client) == cast[pointer](client):
            dec n.value.useCounter, useCount
            n.value.useCounter = max(n.value.useCounter, 0)
            if mustClose(n) or self.shouldClose(n, i):
                client.close()
                self.pool.remove(n)
            elif n.value.useCounter == 0:
                let cpy = n.value
                self.pool.remove(n)
                self.pool.prepend(cpy)
            notify(self)
            return
        inc i
    raise Exception.newException("Trying to return an unmanaged client")

proc cleanup*(self: HttpPool) =
    var stable = false
    while not stable:
        stable = true
        var i = 0
        for n in nodes(self.pool):
            if mustClose(n) or self.shouldClose(n, i):
                let client = n.value.client
                if client.isNil:
                    continue
                client.close()
                self.pool.remove(n)
                stable = false
                notify(self)
                break
        inc i
    
    if not self.pool.head.isNil and len(self.awaiters) == 0:
        self.awaiters = initDeque[Future[void]]()

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


