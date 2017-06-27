package main

import (
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bench "github.com/fjl/goleveldb-bench"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	var (
		testflag     = flag.String("test", "", "tests to run ("+strings.Join(testnames(), ", ")+")")
		sizeflag     = flag.String("size", "500mb", "total amount of value data to write")
		datasizeflag = flag.String("valuesize", "100b", "size of each value")
		keysizeflag  = flag.String("keysize", "32b", "size of each key")
		dirflag      = flag.String("dir", ".", "test database directory")
		logdirflag   = flag.String("logdir", "", "test log output directory")
		run          []string
		cfg          bench.Config
		err          error
	)
	flag.Parse()

	for _, t := range strings.Split(*testflag, ",") {
		if tests[t] == nil {
			log.Fatalf("unknown test %q", t)
		}
		run = append(run, t)
	}
	if len(run) == 0 {
		log.Fatal("no tests to run, use -test to select tests")
	}
	if cfg.Size, err = bench.ParseSize(*sizeflag); err != nil {
		log.Fatal("-size: ", err)
	}
	if cfg.DataSize, err = bench.ParseSize(*datasizeflag); err != nil {
		log.Fatal("-datasize: ", err)
	}
	if cfg.KeySize, err = bench.ParseSize(*keysizeflag); err != nil {
		log.Fatal("-datasize: ", err)
	}

	anyErr := false
	for _, name := range run {
		if err := runTest(*logdirflag, *dirflag, name, cfg); err != nil {
			log.Printf("test %q failed: %v", name, err)
		}
	}
	if anyErr {
		log.Fatal("one ore more tests failed")
	}
}

func runTest(logdir, dbdir, name string, cfg bench.Config) error {
	logfile, err := os.Create(filepath.Join(logdir, name+".json"))
	if err != nil {
		return err
	}
	defer logfile.Close()
	dbdir = filepath.Join(dbdir, "testdb-"+name)
	log.Printf("== running %q", name)
	env := bench.NewEnv(io.MultiWriter(logfile, os.Stdout), cfg)
	return tests[name].Benchmark(dbdir, env)
}

type Benchmarker interface {
	Benchmark(dir string, env *bench.Env) error
}

var tests = map[string]Benchmarker{
	"nobatch":     seqWrite{},
	"batch-50kb":  batchWrite{BatchSize: 50 * 1024},
	"batch-100kb": batchWrite{BatchSize: 100 * 1024},
	"batch-1mb":   batchWrite{BatchSize: 1024 * 1024},
	"batch-5mb":   batchWrite{BatchSize: 5 * 1024 * 1024},
}

func testnames() (n []string) {
	for name, _ := range tests {
		n = append(n, name)
	}
	sort.Strings(n)
	return n
}

type seqWrite struct{}

func (seqWrite) Benchmark(dir string, env *bench.Env) error {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return env.Run(func(key, value []byte, lastCall bool) error {
		if err := db.Put(key, value, nil); err != nil {
			return err
		}
		env.Progress()
		return nil
	})
}

type batchWrite struct {
	BatchSize int
}

func (b batchWrite) Benchmark(dir string, env *bench.Env) error {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	batch := new(leveldb.Batch)
	bsize := 0
	return env.Run(func(key, value []byte, lastCall bool) error {
		batch.Put(key, value)
		bsize += len(value)
		if bsize >= b.BatchSize || lastCall {
			if err := db.Write(batch, nil); err != nil {
				return err
			}
			bsize = 0
			batch.Reset()
			env.Progress()
		}
		return nil
	})
}
