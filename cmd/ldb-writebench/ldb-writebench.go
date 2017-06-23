package main

import (
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	bench "github.com/fjl/goleveldb-bench"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	defSize        = 500 * 1024 * 1024
	defDataSize    = 100
	keySize        = 32
	reportInterval = 1 * time.Second
)

func main() {
	test := flag.String("test", "", "test to run, one of "+strings.Join(testnames(), ", "))
	env := new(bench.Env)
	flag.StringVar(&env.Dir, "dir", "", "test directory")
	flag.IntVar(&env.Size, "size", defSize, "amount of data to write (bytes)")
	flag.IntVar(&env.DataSize, "datasize", defDataSize, "size of each value (bytes)")
	flag.Parse()

	if env.Dir == "" {
		dir, err := ioutil.TempDir("", "ldb-writebench")
		if err != nil {
			log.Fatal("can't make temp dir:", err)
		}
		env.Dir = dir
		defer os.RemoveAll(dir)
	}

	fn := tests[*test]
	if fn == nil {
		log.Fatalf("unknown test %q", *test)
	}
	if err := fn.Benchmark(env); err != nil {
		return
	}
}

type Benchmarker interface {
	Benchmark(*bench.Env) error
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

func (seqWrite) Benchmark(env *bench.Env) error {
	db := env.OpenDB(nil)
	data := make([]byte, env.DataSize)
	env.Start()
	written := 0
	for ; written < env.Size; written += env.DataSize {
		if err := db.Put(nextkey(), data, nil); err != nil {
			return err
		}
		env.Progress(written)
	}
	return db.Close()
}

type batchWrite struct {
	BatchSize int
}

func (b batchWrite) Benchmark(env *bench.Env) error {
	db := env.OpenDB(nil)
	data := make([]byte, env.DataSize)
	env.Start()
	written := 0
	for ; written < env.Size; written += env.DataSize {
		var batch leveldb.Batch
		bsize := 0
		for ; bsize < b.BatchSize && written+bsize < env.Size; bsize += env.DataSize {
			batch.Put(nextkey(), data)
		}
		if err := db.Write(&batch, nil); err != nil {
			return err
		}
		written += bsize
		env.Progress(written)
	}
	return db.Close()
}

var keyrand = rand.New(rand.NewSource(0x1334))

func nextkey() []byte {
	k := make([]byte, keySize)
	keyrand.Read(k)
	return k
}
