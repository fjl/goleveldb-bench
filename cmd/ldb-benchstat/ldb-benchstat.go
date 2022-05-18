package main

import (
	"flag"
	"fmt"
	"time"

	bench "github.com/fjl/goleveldb-bench"
	"gonum.org/v1/gonum/stat"
)

func main() {
	flag.Parse()
	reports := bench.MustReadReports(flag.Args())
	for _, r := range reports {
		var (
			bps       []float64
			weight    []float64
			totalTime float64
			totalSize uint64
		)
		for _, ev := range r.Events {
			bps = append(bps, ev.BPS())
			weight = append(weight, float64(ev.Duration))
			totalTime += float64(ev.Duration) / float64(time.Second)
			totalSize += ev.Delta
		}
		meanBPS, stdBPS := stat.MeanStdDev(bps, weight)
		fmt.Printf("-- %s (%d events)", r.Name, len(r.Events))
		fmt.Printf(" total time: %.4fs\n", totalTime)
		fmt.Printf(" total size: %d bytes\n", totalSize)
		fmt.Printf("  mean mb/s: %.3f (+- %.3f)\n", meanBPS/1024/1024, stdBPS/1024/1024)
	}
}
