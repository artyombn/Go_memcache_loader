# Go_memcache_loader

What's needed from Go learning to rewrite Python memc_loader to Go memc-loader:
- ✅ modules / packages / building
- ✅ variables & constants (step1.go)
- ✅ funcs (step2.go)
- ✅ Go structures (step3.go)
- ✅ Errors handling (step4.go)
- ✅ Protobuf Go
  - https://protobuf.dev/getting-started/gotutorial/
- ✅ Loops patterns in Go (step5.go)
  - https://go.dev/tour/flowcontrol/1


go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
protoc --go_out=. --go_opt=paths=source_relative appsinstalled/appsinstalled.proto