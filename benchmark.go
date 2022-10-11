package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/mdogan/hdrhistogram"
)

type result struct {
	*hdrhistogram.Histogram
	duration time.Duration
}

var totalOperations uint64

func benchmark(m *hazelcast.Map, wg *sync.WaitGroup, ctx context.Context, ch chan *result) {
	hist := hdrhistogram.New(1, int64(time.Second), 3)
	value := make([]byte, valueSize)
	rnd := rand.New(rand.NewSource(rand.Int63()))
	var err error
	result := result{Histogram: hist}

	defer func() {
		ch <- &result
		wg.Done()
	}()

	for i := 0; i < requests/clients; i++ {
		select {
		case <-ctx.Done():
			return
		default:

			op := rnd.Intn(setRatio + getRatio)
			k := rnd.Intn(keyCount)
			start := time.Now()

			if op < getRatio {
				_, err = m.Get(ctx, k)
			} else {
				err = m.Set(ctx, k, value)
			}
			elapsed := time.Now().Sub(start)
			_ = hist.RecordValue(elapsed.Nanoseconds())
			result.duration += elapsed

			if i%100 == 0 {
				atomic.AddUint64(&totalOperations, 100)
			}

			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}
