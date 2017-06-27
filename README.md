goleveldb-bench tests the performance of random writes to a goleveldb database. To get
started clone this repo to your GOPATH, then get the dependencies with govendor and
compile:

    govendor sync
    go install -v ./...

You can run benchmarks with `ldb-writebench`:

    mkdir datasets/mymachine-10gb
    ldb-writebench -size 10gb -logdir datasets/mymachine-10gb -test nobatch,batch-100kb

Plot the result with `ldb-benchplot`:

    ldb-benchplot -out 10gb.svg datasets/mymachine-10gb

LevelDB databases are left on disk for inspection. You can remove them using

    rm -r testdb-*
