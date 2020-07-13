package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	bench "github.com/fjl/goleveldb-bench"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	var (
		testflag     = flag.String("test", "", "tests to run ("+strings.Join(testnames(), ", ")+")")
		sizeflag     = flag.String("size", "500mb", "total amount of value data to write")
		datasizeflag = flag.String("valuesize", "100b", "size of each value")
		keysizeflag  = flag.String("keysize", "32b", "size of each key")
		dirflag      = flag.String("dir", ".", "test database directory")
		logdirflag   = flag.String("logdir", ".", "test log output directory")
		deletedbflag = flag.Bool("deletedb", false, "delete databases after test run")

		// PProf related flags
		pprofCPUflag  = flag.Bool("pprof.cpu", false, "enable cpu performance profiling")
		pprofAddrFlag = flag.String("pprof.addr", "", "pprof server listening interface(empty = disabled)")
		pprofPortFlag = flag.Int("pprof.port", 6060, "tcp port number on which to start pprof server(0 = random)")

		run []string
		cfg bench.ReadConfig
		err error
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
	cfg.LogPercent = true

	if err := os.MkdirAll(*logdirflag, 0755); err != nil {
		log.Fatal("can't create log dir: %v", err)
	}

	// Setup pprof web interface if required.
	if *pprofAddrFlag != "" {
		endpoint := fmt.Sprintf("%v:%d", *pprofAddrFlag, *pprofPortFlag)
		go func() {
			l, err := net.Listen("tcp", endpoint)
			if err != nil {
				log.Println("Failed to start pprof server")
				return
			}
			log.Println("PProf web interface opened", "endpoint", l.Addr().(*net.TCPAddr))
			fmt.Println(http.Serve(l, nil))
		}()
	}

	anyErr := false
	for _, name := range run {
		var (
			dbdir    string
			createdb bool
		)
		// The given dir points to an existent directory, assume it's
		// a old database for read testing.
		if isDir(*dirflag) && fileExist(filepath.Join(*dirflag, "testing.key")) {
			if strings.Contains(*dirflag, "filter") != strings.Contains(name, "filter") {
				log.Printf("Skip test %s. Incompatible database", name)
				continue
			}
			dbdir = *dirflag
		} else {
			dbdir, createdb = filepath.Join(*dirflag, "testdb-"+name), true
		}
		if err := os.MkdirAll(dbdir, 0755); err != nil {
			log.Fatal("can't create keyfile dir: %v", err)
		}
		if err := runTest(*logdirflag, dbdir, name, createdb, *pprofCPUflag, cfg); err != nil {
			log.Printf("test %q failed: %v", name, err)
			anyErr = true
		}
		if *deletedbflag {
			os.RemoveAll(dbdir)
		}
	}
	if anyErr {
		log.Fatal("one ore more tests failed")
	}
}

func runTest(logdir, dbdir, name string, createdb bool, pprofCPU bool, cfg bench.ReadConfig) error {
	cfg.TestName = name
	logfile, err := os.Create(filepath.Join(logdir, name+time.Now().Format(".2006-01-02-15:04:05")+".json"))
	if err != nil {
		return err
	}
	defer logfile.Close()

	var (
		kw    io.Writer
		kr    io.Reader
		reset func()
		kfile = filepath.Join(dbdir, "testing.key")
	)
	if !createdb {
		keyfile, err := os.Open(kfile)
		if err != nil {
			return err
		}
		defer keyfile.Close()
		kr = keyfile
	} else {
		keyfile, err := os.Create(kfile)
		if err != nil {
			return err
		}
		defer keyfile.Close()
		kw, kr = keyfile, keyfile
		reset = func() {keyfile.Seek(0, io.SeekStart)}
	}

	log.Printf("== running %q", name)
	env := bench.NewReadEnv(logfile, kr, kw, reset, cfg)

	// Setup cpu profiling writer if required
	if pprofCPU {
		cpufile, err := os.Create(filepath.Join(logdir, name+time.Now().Format(".2006-01-02-15:04:05")+".cpu"))
		if err != nil {
			return err
		}
		defer cpufile.Close()
		env = env.WithCPUProfiler(cpufile)
	}
	return tests[name].Benchmark(dbdir, env)
}

type Benchmarker interface {
	Benchmark(dir string, env *bench.ReadEnv) error
}

var tests = map[string]Benchmarker{
	"random-read": randomRead{},
	"random-read-filter": randomRead{Options: opt.Options{
		Filter: filter.NewBloomFilter(10),
	}},
	"random-read-bigcache": randomRead{Options: opt.Options{
		BlockCacheCapacity:     100 * opt.MiB,
	}},
	"random-read-bigcache-filter": randomRead{Options: opt.Options{
		BlockCacheCapacity:     100 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
	}},
	"random-read-bigcache-filter-no-seekcomp": randomRead{Options: opt.Options{
		BlockCacheCapacity:     100 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
	}},
	"random-read-bigcache-filter-no-seekcomp-filecache": randomRead{Options: opt.Options{
		BlockCacheCapacity:     100 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
		OpenFilesCacheCapacity: 10000, // Need to raise the allowance in OS
	}},
}

func testnames() (n []string) {
	for name := range tests {
		n = append(n, name)
	}
	sort.Strings(n)
	return n
}

type randomRead struct {
	Options opt.Options
}

func (b randomRead) Benchmark(dir string, env *bench.ReadEnv) error {
	db, err := leveldb.OpenFile(dir, &b.Options)
	if err != nil {
		return err
	}
	defer db.Close()
	return env.Run(func(key, value string, lastCall bool) error {
		if err := db.Put([]byte(key), []byte(value), nil); err != nil {
			return err
		}
		return nil
	}, func(key string) error {
		if value, err := db.Get([]byte(key), nil); err != nil {
			return err
		} else {
			env.Progress(len(value))
		}
		return nil
	})
}

func fileExist(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func isDir(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		return false
	}
	return f.Mode().IsDir()
}
