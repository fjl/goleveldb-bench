package bench

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aristanetworks/goarista/monotime"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const emitInterval = 20 * 1024 // bytes

type Env struct {
	Size     int    `json:"size"`
	DataSize int    `json:"datasize"`
	Dir      string `json:"-"`

	startTime, lastTime time.Duration
	lastSize            int
	out                 *json.Encoder
}

type Progress struct {
	Written  int           `json:"written"`  // total bytes written so far
	Delta    int           `json:"delta"`    // bytes written since last event
	Duration time.Duration `json:"duration"` // time in ns since last event
}

// BPS returns the 'write speed' in bytes/s.
func (ev Progress) BPS() float64 {
	return (float64(ev.Delta) / float64(ev.Duration)) * float64(time.Second)
}

func (env *Env) OpenDB(o *opt.Options) *leveldb.DB {
	db, err := leveldb.OpenFile(env.Dir, o)
	if err != nil {
		log.Fatal("can't open database:", err)
	}
	return db
}

func (env *Env) Start() {
	env.startTime = mononow()
	env.lastTime = env.startTime
	env.out = json.NewEncoder(os.Stdout)
}

func (env *Env) Progress(written int) {
	env.writeProgress(mononow(), written, false)
}

func (env *Env) writeProgress(now time.Duration, written int, force bool) {
	d := now - env.lastTime
	dw := written - env.lastSize
	if dw > 0 && (dw > emitInterval || force) {
		p := Progress{Written: written, Delta: dw, Duration: d}
		env.out.Encode(&p)
		env.lastTime = now
		env.lastSize = written
	}
}

func mononow() time.Duration {
	return time.Duration(monotime.Now())
}

// ReadProgress reads JSON progress events in a file.
func ReadProgress(file string) ([]Progress, error) {
	fd, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	var pp []Progress
	dec := json.NewDecoder(fd)
	for {
		var p Progress
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return pp, err
		}
		pp = append(pp, p)
	}
	return pp, nil
}

type Report struct {
	Name   string
	Events []Progress
}

// MustReadReports reads all given progress event files.
func MustReadReports(files []string) []Report {
	var reports []Report
	for _, file := range files {
		p, err := ReadProgress(file)
		if err != nil {
			log.Fatalf("%s: %v", file, err)
		}
		reports = append(reports, Report{
			Events: p,
			Name:   strings.TrimSuffix(filepath.Base(file), filepath.Ext(file)),
		})
	}
	return reports
}
