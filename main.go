package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

    
	"github.com/mdogan/hdrhistogram"
)

func main() {
    println("Configuring Hazelcast client")

    parseFlags()


	client, err := newClient()
	if err != nil {
		exit(err)
	}
	ctx := context.TODO()

	defer client.Shutdown(ctx)

	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		exit(err)
	}

	ctx, cancelF := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	resultCh := make(chan *result, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go benchmark(m, &wg, ctx, resultCh)
	}

	wgt := sync.WaitGroup{}
	wgt.Add(1)
	go printThroughput(ctx, &wgt)

	handleSignal(cancelF)
	wg.Wait()
	cancelF()

	wgt.Wait()
	fmt.Printf("%100s\r", "")

	total := hdrhistogram.New(1, int64(time.Second), 3)
	ops := float64(0)
	for i := 0; i < clients; i++ {
		r := <-resultCh
		total.Merge(r.Histogram)
		ops += float64(r.Histogram.TotalCount()) / float64(r.duration.Microseconds())
	}
	setCount := (setRatio * requests) / (setRatio + getRatio)
	fmt.Printf("Set Count\t\t: %10d\n", setCount)
	fmt.Printf("Get Count\t\t: %10d\n", requests-setCount)
	fmt.Printf("Keyspace Range\t\t: %10d\n", keyCount)
	fmt.Printf("Value Size\t\t: %10d\n", valueSize)
	fmt.Printf("Number of Requests\t: %10d\n", requests)
	fmt.Printf("Number of Threads\t: %10d\n", clients)

	fmt.Println(buildHistogramString(total, 1000))
	fmt.Printf("Throughput: %.3F op/s\n\n", ops*1000000)
}

func printThroughput(ctx context.Context, w *sync.WaitGroup) {
	defer w.Done()
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			diff := time.Now().Sub(start)
			t := float64(atomic.LoadUint64(&totalOperations)) / float64(diff.Microseconds())
			fmt.Printf("Throughput: %.3F op/s\r", t*1000000)
		}
	}
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(-1)
}

func handleSignal(cancelF context.CancelFunc) {
	var stopCh = make(chan os.Signal)
	signal.Notify(stopCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		s := <-stopCh
		fmt.Printf("Stopped via signal %v\n", s)
		cancelF()
	}()
}

func buildHistogramString(h *hdrhistogram.Histogram, outputValueUnitScalingRatio float64) string {
	builder := &strings.Builder{}
	d := h.CumulativeDistribution()
	_, _ = fmt.Fprintf(builder, "\n%12s %12s %10s\n", "Value", "Percentile", "TotalCount")

	for _, b := range d {
		_, _ = fmt.Fprintf(builder, "%12.3f %12.8f %10d\n", float64(b.ValueAt)/outputValueUnitScalingRatio,
			b.Quantile/float64(100), b.Count)
	}

	_, _ = fmt.Fprintf(builder, "%-8s%12.3F, %10s%12.3F]\n", "#[Mean =", h.Mean(), "StdDev =", h.StdDev())
	_, _ = fmt.Fprintf(builder, "%-8s%12.3F, %10s%12d]\n", "#[Max  =", float64(h.Max())/outputValueUnitScalingRatio, "Total  =", h.TotalCount())

	return builder.String()
}
