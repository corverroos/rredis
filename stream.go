package rredis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/mediocregopher/radix/v4"
)

const (
	fieldType      = "type"
	fieldForeignID = "foreign_id"
	fieldTimestamp = "timestamp"
	fieldData      = "data"

	timeFormat = time.RFC3339Nano
)

// NewStream returns a reflex stream capable of streaming (reading) from and writing to a single redis stream.
//
// Note that radix.Client is shared amongst users of the Stream struct and that some implementations like radix.Con
// does blocking XREAD calls. For concurrent usage, use radix.Pool instead.
//
// TODO(corver): Add support for multiple names/keys.
func NewStream(cl radix.Client, namespace, name string, opts ...option) *Stream {
	s := &Stream{
		key: fmt.Sprintf("reflex_streams|%s|%s", namespace, name),
		cl:  cl,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Stream represents a redis stream.
type Stream struct {
	// key (name) of the redis stream.
	key string

	noBlock bool

	// cl is the redis client.
	cl radix.Client
}

// Insert inserts the data as a new redis stream event.
func (s *Stream) Insert(ctx context.Context, data []byte, opts ...ioption) error {
	args := []string {
		s.key, "*",
		fieldData, string(data),
		fieldTimestamp, time.Now().Format(timeFormat),
	}

	for _, opt := range opts {
		opt(&args)
	}

	return s.cl.Do(ctx, radix.Cmd(nil, "XADD", args...))
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from the redis stream after the provided cursor (redis stream ID).
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
func (s *Stream) Stream(ctx context.Context, after string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	sopts := new(reflex.StreamOptions)
	for _, opt := range opts {
		opt(sopts)
	}

	var conf radix.StreamConfig

	if sopts.Lag > 0 {
		return nil, errors.New("lag option not supported")
	} else if sopts.StreamFromHead {
		conf.Latest = true
	} else if after != "" {
		var err error
		conf.After, err = parseStreamEntryID(after)
		if err != nil {
			return nil, errors.Wrap(err, "failed parsing redis stream entry id: "+after)
		}
	}

	return streamclient{
		ctx:    ctx,
		toHead: sopts.StreamToHead,
		reader: radix.StreamReaderConfig{Count: 1000, NoBlock: s.noBlock}.
			New(s.cl, map[string]radix.StreamConfig{s.key: conf}),
	}, nil
}

type streamclient struct {
	ctx    context.Context
	reader radix.StreamReader
	toHead bool
}

func (s streamclient) Recv() (*reflex.Event, error) {
	for {
		_, entry, err := s.reader.Next(s.ctx)
		if errors.Is(err, radix.ErrNoStreamEntries) {
			if s.toHead {
				return nil, reflex.ErrHeadReached
			}

			time.Sleep(time.Millisecond * 100) // Do not spin, TODO(corver): make this configurable.
			continue
		} else if err != nil {
			return nil, errors.Wrap(err, "redis stream next")
		}

		return parseEvent(entry)
	}
}

func parseEvent(e radix.StreamEntry) (*reflex.Event, error) {
	var (
		err     error
		gotdata bool
		res     = reflex.Event{ID: e.ID.String()}
	)
	for _, field := range e.Fields {
		switch field[0] {
		case fieldType:
			i, err := strconv.Atoi(field[1])
			if err != nil {
				return nil, errors.Wrap(err, "failed parsing event type")
			}
			res.Type = eventType(i)
		case fieldForeignID:
			res.ForeignID = field[1]
		case fieldTimestamp:
			res.Timestamp, err = time.Parse(timeFormat, field[1])
			if err != nil {
				return nil, errors.Wrap(err, "failed parsing event timestamp")
			}
		case fieldData:
			gotdata = true
			res.MetaData = []byte(field[1])
		default:
			return nil, errors.New("unknown field " + field[0])
		}
	}

	if !gotdata {
		return nil, errors.New("missing data field")
	}

	return &res, nil
}

type eventType int

func (t eventType) ReflexType() int {
	return int(t)
}

func parseStreamEntryID(val string) (res radix.StreamEntryID, err error) {
	split := strings.Split(val, "-")
	if len(split) != 2 {
		return res, errors.New("invalid stream entry id")
	}

	res.Time, err = strconv.ParseUint(split[0], 10, 64)
	if err != nil {
		return res, errors.New("invalid stream entry id time")
	}

	res.Seq, err = strconv.ParseUint(split[1], 10, 64)
	if err != nil {
		return res, errors.New("invalid stream entry id seq")
	}

	return res, nil
}
