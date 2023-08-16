# MIT_6.824_Labs

It's my MIT 6.824 lab repo

## Nessary Instruct (In `Lab1/src/main` Dir)
```shell
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go
```

## P.S.
Have to change wc.so location in `Lab1/src/main/mrworker.go`
