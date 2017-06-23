goleveldb-bench tests the performance of random writes to a goleveldb database. To get
started clone this repo to your GOPATH, then get the dependencies with govendor and
compile:

    govendor sync
    go install -v ./...

You can run benchmarks with `ldb-writebench`:

    ldb-writebench -size 10gb -dir database-dir-1 -test nobatch > 10gb-nobatch.json
    ldb-writebench -size 10gb -dir database-dir-2 -test batch-100kb > 10gb-batch-100kb.json

Plot the result with `ldb-benchplot`:

    ldb-benchplot -out 10gb.svg 10gb-nobatch.json 10gb-batch-100kb.json
