package bench

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aristanetworks/goarista/monotime"
)

const emitInterval = 500 * 1024 // bytes

type Config struct {
	Size     int `json:"size"`     // total size of values to write
	KeySize  int `json:"keysize"`  // size of each key written
	DataSize int `json:"datasize"` // size of each value written
}

type Env struct {
	cfg Config
	// generating keys and values
	key, value []byte
	rand       *rand.Rand
	out        *json.Encoder
	// reporting
	mu                   sync.Mutex
	startTime, lastTime  time.Duration
	written, lastWritten int
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

func NewEnv(output io.Writer, cfg Config) *Env {
	return &Env{
		cfg:   cfg,
		out:   json.NewEncoder(output),
		key:   make([]byte, cfg.KeySize),
		value: make([]byte, cfg.DataSize),
	}
}

// Run calls write repeatedly with random keys and values.
// The write function should perform a database write and call Progress when
// data has actually been flushed to disk.
func (env *Env) Run(write func(key, value string, lastCall bool) error) error {
	env.start()
	written := 0
	for {
		env.rand.Read(env.key)
		env.rand.Read(env.value)
		written += env.cfg.DataSize
		end := written >= env.cfg.Size
		err := write(string(env.key), string(env.value), end)
		if err != nil || end {
			return err
		}
	}
}

func (env *Env) start() {
	env.written, env.lastWritten = 0, 0
	env.rand = rand.New(rand.NewSource(0x1334))
	env.startTime = mononow()
	env.lastTime = env.startTime
}

// Progress writes a JSON progress event to the environment's output writer.
func (env *Env) Progress(w int) {
	now := mononow()
	env.mu.Lock()
	defer env.mu.Unlock()
	env.written += w
	d := now - env.lastTime
	dw := env.written - env.lastWritten
	if dw > 0 && dw > emitInterval {
		p := Progress{Written: env.written, Delta: dw, Duration: d}
		env.out.Encode(&p)
		env.lastTime = now
		env.lastWritten = env.written
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
