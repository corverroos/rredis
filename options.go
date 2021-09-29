package rredis

import (
	"fmt"
	"github.com/luno/reflex"
	"time"
)

type option func(*Stream)

// WithNoBlock returns an option to disable blocking when doing XREAD calls.
// This has a negative performance impact.
func WithNoBlock() option {
	return func(s *Stream) {
		s.noBlock = true
	}
}


type ioption func(args *[]string)

func WithEventType(typ reflex.EventType) ioption {
	return func(args *[]string) {
		*args = append(*args, []string{fieldType, fmt.Sprint(typ.ReflexType())}...)
	}
}

func WithForeignID(foreignID string ) ioption {
	return func(args *[]string) {
		*args = append(*args, []string{fieldForeignID, foreignID}...)
	}
}

func WithTimestamp(t time.Time ) ioption {
	return func(args *[]string) {
		(*args)[5] = t.Format(timeFormat)
	}
}
