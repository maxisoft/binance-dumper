import std/atomics

type
    CancellationTokenValue* = object
        cancelled: Atomic[bool]

    CancellationTokenRef* = ref CancellationTokenValue

    StaticCancellationTokenCancelled* = object
    StaticCancellationTokenNotCancelled* = object

    CancellationToken* = CancellationTokenValue | CancellationTokenRef | StaticCancellationTokenCancelled | StaticCancellationTokenNotCancelled

    CancellationTokenAlreadyCancelledError* = object of CatchableError

proc newCancellationToken*(): CancellationTokenRef =
    result.new()

proc newCancellationToken*(value: bool): CancellationTokenRef =
    result.new()
    result.cancelled.store(value)

template cancel() =
    var expected = false
    let success = self.cancelled.compareExchange(expected, true)
    if not success and throws:
        raise CancellationTokenAlreadyCancelledError.newException("")

proc cancel*(self: CancellationTokenRef, throws = true) =
    cancel()

proc cancel*(self: var CancellationTokenValue, throws = true) =
    cancel()

proc cancel*(self: StaticCancellationTokenCancelled | StaticCancellationTokenNotCancelled, throws = true) =
    discard

proc cancelled*(self: CancellationTokenValue | CancellationTokenRef): bool {.inline.} =
    result = self.cancelled.load()

proc cancelled*(self: var CancellationTokenValue): bool {.inline.} =
    result = self.cancelled.load()

proc cancelled*(self: StaticCancellationTokenCancelled): bool {.inline.} =
    result = true

proc cancelled*(self: StaticCancellationTokenNotCancelled): bool {.inline.} =
    result = false
