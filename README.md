# Go MemcLoad
### Rewrote Python memc_loader to Go memc_loader to process TSV logs and load them into memcached concurrently

What was learnt from Golang to rewrite Python memc_loader to Go memc_loader:
- ✅ modules / packages / building
- ✅ variables & constants (step1.go)
- ✅ funcs (step2.go)
- ✅ Go structures (step3.go)
- ✅ Errors handling (step4.go)
- ✅ Protobuf Go
  - https://protobuf.dev/getting-started/gotutorial/
- ✅ Loops patterns in Go (step5.go)
  - https://go.dev/tour/flowcontrol/1
- ✅ Command-Line Flags in Go (step6.go)
  - https://gobyexample.com/command-line-flags
- ✅ Goroutine (step7.go)


#### The project includes two Go versions:
* single-threaded (`memc_load.go`)
* multi-threaded (`memc_load_concurrency.go`)

### Requirements
- Go (https://go.dev/doc/install)
- protocol buffers (https://protobuf.dev/downloads/)
- Go protoc plugin:
  - ```go install google.golang.org/protobuf/cmd/protoc-gen-go@latest```
- Generate Go file from .proto
  - ```protoc --go_out=. --go_opt=paths=source_relative appsinstalled/appsinstalled.proto```

### Run Examples
#### Single-threaded version:
```shell
go run memc_load.go --pattern "data/*.tsv.gz" --dry --log "logs/memc_load_single.log"
```

#### Multi-threaded version:
```shell
go run memc_load_concurrency.go --pattern "data/*.tsv.gz" --dry --log "logs/memc_load_concurrency.log"
```

### Options Parser:
```
--pattern — path to .tsv.gz files (glob pattern)
--dry — if set, no data is sent to Memcached, only logged
--log — path to the log file
```

### Example Input File
```
idfa e7e1a50c0ec2747ca56cd9e1558c0d7c 67.7835424444 -22.8044005471 7942,8519,4232,3
idfa f5ae5fe6122bb20d08ff2c2ec43fb4c4 -104.68583244 -51.24448376 4877,7862,7181,6
gaid 3261cf44cbe6a00839c574336fdf49f6 137.790839567 56.8403675248 7462,1115,5205,6
``` 

### Logging
##### Single-threaded version:
```
DEBUG 2025/06/05 17:57:47 Memc loader started with pattern: data/*.tsv.gz
DEBUG 2025/06/05 17:57:47 Processing data/20170929000000.tsv.gz
...
...
DEBUG 2025/06/05 17:59:18 Acceptable error rate (0.000000). Successful load
DEBUG 2025/06/05 17:59:18 Total processed: 10269498, total errors: 0, total execution time: 90.939864 sec
```

##### Multi-threaded version:
```
DEBUG 2025/06/05 18:00:56 Memc loader started with pattern: data/*.tsv.gz
DEBUG 2025/06/05 18:00:56 Processing data/20170929000000.tsv.gz
DEBUG 2025/06/05 18:00:56 Processing data/20170929000100.tsv.gz
DEBUG 2025/06/05 18:00:56 Processing data/20170929000200.tsv.gz
...
...
DEBUG 2025/06/05 18:02:00 Acceptable error rate (0.000000). Successful load
DEBUG 2025/06/05 18:02:00 Finished processing data/20170929000100.tsv.gz: processed=3424477, errors=0, execution time: 63.786469 sec
DEBUG 2025/06/05 18:02:00 Total processed: 10269498, total errors: 0, total execution time: 63.786682 sec
```

### Links to Test Files
- [File 1](https://cloud.mail.ru/public/2hZL/Ko9s8R9TA)
- [File 2](https://cloud.mail.ru/public/DzSX/oj8RxGX1A)
- [File 3](https://cloud.mail.ru/public/LoDo)




