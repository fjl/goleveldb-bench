package main

import (
	"flag"
	"fmt"
	"log"
	"math"

	bench "github.com/fjl/goleveldb-bench"
	"github.com/gonum/plot"
	"github.com/gonum/plot/plotter"
	"github.com/gonum/plot/plotutil"
	"github.com/gonum/plot/vg"
)

func main() {
	var (
		width  = flag.Int("width", 15, "with of plot in cm")
		height = flag.Int("height", 10, "height of plot in cm")
		out    = flag.String("out", "", "output filename")
	)
	flag.Parse()
	reports := bench.MustReadReports(flag.Args())

	p, err := plot.New()
	if err != nil {
		log.Fatal(err)
	}
	plotBPS(p, reports)
	if err := p.Save(vg.Length(*width)*vg.Centimeter, vg.Length(*height)*vg.Centimeter, *out); err != nil {
		log.Fatal(err)
	}
}

// reduceEvents aggregates progress events so there are ~n total events.
// This smoothes out the line in the plot.
func reduceEvents(events []bench.Progress, n int) []bench.Progress {
	group := len(events) / n
	if group <= 1 || len(events) == 0 {
		return events
	}
	grouped := make([]bench.Progress, 0, n)
	for i, ev := range events {
		if i%group == 0 {
			grouped = append(grouped, bench.Progress{})
		}
		end := len(grouped) - 1
		grouped[end].Delta += ev.Delta
		grouped[end].Duration += ev.Duration
		grouped[end].Written = ev.Written
	}
	return grouped
}

// plotBPS adds BPS vs. database size plots for all reports.
func plotBPS(plt *plot.Plot, reports []bench.Report) {
	plt.X.Tick.Marker = megabyteTicks{unit: "mb"}
	plt.X.Label.Text = "database size"
	// plt.Y.Scale = plot.LogScale{}
	plt.Y.Label.Text = "write speed"
	plt.Y.Tick.Marker = megabyteTicks{unit: "mb/s"}

	i := 0
	for _, r := range reports {
		if len(r.Events) == 0 {
			log.Print("Warning: report %s has 0 progress events", r.Name)
			continue
		}
		evs := reduceEvents(r.Events, 400)
		l, err := plotter.NewLine(bpsLine(evs))
		if err != nil {
			log.Fatal(err)
		}
		l.Color = plotutil.Color(i)
		plt.Add(l)
		plt.Legend.Add(r.Name, l)
		i++
	}
}

// bpsLine plots X = db size against Y = bytes per second written.
type bpsLine []bench.Progress

func (l bpsLine) Len() int {
	return len(l)
}

func (l bpsLine) XY(i int) (float64, float64) {
	x := float64(l[i].Written)
	return x, l[i].BPS()
}

// megabyteTicks emits axis labels corresponding to megabytes written.
type megabyteTicks struct{ unit string }

func (mt megabyteTicks) Ticks(min, max float64) (t []plot.Tick) {
	const numLabels = 5
	mag := nextPowerOfTwo(max - min)
	dist := nextPowerOfTwo(mag / numLabels)
	for s := nextPowerOfTwo(min); s < max; s += dist {
		t = append(t, plot.Tick{Value: s, Label: fmt.Sprintf("%.2f %s", s/1024/1024, mt.unit)})
	}
	t = append(t, plot.Tick{Value: max, Label: fmt.Sprintf("%.2f %s", max/1024/1024, mt.unit)})
	return t
}

func nextPowerOfTwo(f float64) float64 {
	return math.Pow(2, math.Ceil(math.Log2(f)))
}
