package rredis_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/luno/jettison/jtest"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/corverroos/rredis"
	"github.com/luno/fate"
	"github.com/luno/reflex"
	"github.com/matryer/is"
	"github.com/mediocregopher/radix/v4"
)

const (
	ns     = "namespace"
	cursor = "cursor"
	stream = "stream"
)

func randStr() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func setup(t *testing.T) (context.Context, radix.Conn, *is.I) {
	rand.Seed(time.Now().UnixNano())
	is := is.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c, err := radix.Dial(ctx, "tcp", "127.0.0.1:6379")
	is.NoErr(err)

	t.Cleanup(func() {
		is.NoErr(c.Close())
	})

	clean := func() {
		var res []string
		is.NoErr(c.Do(ctx, radix.Cmd(&res, "KEYS", "reflex*")))
		for _, b := range res {
			is.NoErr(c.Do(ctx, radix.Cmd(nil, "DEL", b)))
		}
	}

	clean()
	t.Cleanup(clean)

	return ctx, c, is
}

func TestBasicStream(t *testing.T) {
	ctx, c, ii := setup(t)
	s := rredis.NewStream(c, ns, stream, rredis.WithNoBlock()) // Use NoBlock since we use a single radix.Con and concurrent goroutines.

	insertRand := func(ii *is.I, count int) {
		ii.Helper()

		for i := 0; i < count; i++ {
			ii.NoErr(s.Insert(ctx, []byte(randStr())))
		}
	}

	assertRand := func(ii *is.I, sc reflex.StreamClient, count int) {
		ii.Helper()

		for i := 0; i < count; i++ {
			e, err := sc.Recv()
			ii.NoErr(err)
			ii.True(len(e.MetaData) > 0) // Not empty
		}
	}

	log.SetFlags(log.Lmicroseconds)

	// Start async consumer that first blocks
	var wg sync.WaitGroup
	sc0, err := s.Stream(ctx, "")
	ii.NoErr(err)
	wg.Add(1)
	go func() {
		assertRand(ii, sc0, 7)
		wg.Done()
	}()

	sc1, err := s.Stream(ctx, "")
	ii.NoErr(err)

	insertRand(ii, 5)
	assertRand(ii, sc1, 5)
	insertRand(ii, 1)
	insertRand(ii, 1)
	assertRand(ii, sc1, 2)

	sc2, err := s.Stream(ctx, "")
	ii.NoErr(err)
	assertRand(ii, sc2, 7)

	wg.Wait()
}

func TestRobust(t *testing.T) {
	ctx, c, ii := setup(t)
	s := rredis.NewStream(c, ns, stream, rredis.WithNoBlock()) // Use NoBlock since we use a single radix.Con and concurrent goroutines.
	store := rredis.NewCursorStore(c, ns)

	runOnce := func(ii *is.I, from, to int) {
		ii.Helper()

		expect := from
		consumer := reflex.NewConsumer("test", func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
			ii.Equal(string(e.MetaData), fmt.Sprint(expect))
			expect++
			return nil
		})

		for i := from; i < to; i++ {
			ii.NoErr(s.Insert(ctx, []byte(fmt.Sprint(i))))
		}

		err := reflex.Run(ctx, reflex.NewSpec(s.Stream, store, consumer, reflex.WithStreamToHead()))
		jtest.Require(t, reflex.ErrHeadReached, err)
	}

	runOnce(ii, 0, 10)

	val1, err := store.GetCursor(ctx, "test")
	ii.NoErr(err)

	runOnce(ii, 11, 20)

	val2, err := store.GetCursor(ctx, "test")
	ii.NoErr(err)

	ii.True(val1 < val2)
}

func TestFields(t *testing.T) {
	ctx, c, ii := setup(t)
	s := rredis.NewStream(c, ns, stream)

	t0 := time.Now().Truncate(time.Nanosecond)

	ii.NoErr(s.Insert(ctx, []byte("1"), rredis.WithEventType(testType(1))))
	ii.NoErr(s.Insert(ctx, []byte("2"), rredis.WithForeignID("2")))
	ii.NoErr(s.Insert(ctx, []byte("3"), rredis.WithTimestamp(t0)))

	sc, err := s.Stream(ctx, "")
	ii.NoErr(err)

	e1, err := sc.Recv()
	ii.NoErr(err)
	ii.Equal(e1.MetaData, []byte("1"))
	ii.True(reflex.IsAnyType(e1.Type, testType(1)))

	e2, err := sc.Recv()
	ii.NoErr(err)
	ii.Equal(e2.MetaData, []byte("2"))
	ii.Equal(e2.ForeignID, "2")

	e3, err := sc.Recv()
	ii.NoErr(err)
	ii.Equal(e3.MetaData, []byte("3"))
	ii.Equal(e3.Timestamp, t0)
}

func TestBench01(t *testing.T) {
	ctx, c, ii := setup(t)

	const (
		total   = 10000
		payload = 64
	)

	b := make([]byte, payload)
	_, err := rand.Read(b)
	ii.NoErr(err)

	s := rredis.NewStream(c, ns, stream)
	ii.NoErr(err)

	t0 := time.Now()

	go func() {
		for i := 0; i < total; i++ {
			err := s.Insert(ctx, b)
			ii.NoErr(err)
		}
		delta := time.Since(t0)
		fmt.Printf("Publish done after %s, %.1f msgs/sec\n", delta, total/delta.Seconds())
	}()

	sc, err := s.Stream(ctx, "")
	ii.NoErr(err)

	for i := 0; i < total; i++ {
		_, err := sc.Recv()
		ii.NoErr(err)
	}

	delta := time.Since(t0)

	fmt.Printf("Duration=%dms, Total=%d, Payload=%d bytes, Throughput=%.0f msgs/sec\n", delta.Milliseconds(), total, payload, total/delta.Seconds())
}

type testType int

func (t testType) ReflexType() int {
	return int(t)
}
