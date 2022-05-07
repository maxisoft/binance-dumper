# Bitfinex Dumper
Save binance futures's *long short ratios*, *open interst*, *buy sell ratio* of all *USDT* pairs.

## Description
This project save multiples multiple binance futures data sources into [csv](https://en.wikipedia.org/wiki/Comma-separated_values) files.  
The program target data source with no permanent retention on binance side.  
Such csv may allow one to develop investment strategies, charts, analysis, ... without relying on external services (ie just from your own and raw data from the cex).

## Data sources
Currently all those data from the usdt pairs are collected from the api:

- [topLongShortAccountRatio](https://binance-docs.github.io/apidocs/futures/en/#top-trader-long-short-ratio-accounts=)
- [topLongShortPositionRatio](https://binance-docs.github.io/apidocs/futures/en/#top-trader-long-short-ratio-positions=)
- [globalLongShortAccountRatio](https://binance-docs.github.io/apidocs/futures/en/#long-short-ratio=)
- [takerlongshortRatio](https://binance-docs.github.io/apidocs/futures/en/#taker-buy-sell-volume=)
- [openInterestHist](https://binance-docs.github.io/apidocs/futures/en/#taker-buy-sell-volume=)

The resulting csv files use the same columns (but the pair column) as specified by the api docs.  

## Requirement
A standard working nim environment with
- recent nim version (tested with **nim 1.6**)
- C compiler
- nimble
- sqlite devel lib
- ssl devel lib

## Dependencies
```sh
nimble install ws asynctools sorta
```

## Compilation
```sh
nim c -d:release --stackTrace:on --opt:speed -d:ssl --app:console --filenames:canonical -o:binance_dumper ./src/main.nim
```

## Usage
Start `./binance_dumper` and it'll write csv files into the **current working directory**.  

One should use external restart mechanical/loop such as [systemd](https://github.com/maxisoft/binance-dumper/tree/dev/systemd) or [docker](https://github.com/maxisoft/binance-dumper/pkgs/container/binance_dumper%2Fbinance_dumper) to restart the soft in case of crash (eg internet disconnections)


## FAQ
### How can I configure tracked pairs ?
Zero configuration mode for now so all USDT pairs found at program startup are tracked.

### Program use too much cpu/ram
By default the program use a pool of 20 http connections to do his tasks.  
In some case (like a fresh program start) it may use a larger usage resource than usual to download all the data. To reduce this, one can set the environment variable `BINANCE_SCHEDULER_CONCURRENT_TASK=4`
