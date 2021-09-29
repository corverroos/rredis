package rredis

import (
	"context"
	"fmt"

	"github.com/luno/jettison/errors"
	"github.com/mediocregopher/radix/v4"
)

func NewCursorStore(cl radix.Client, namespace string) CursorStore {
	return CursorStore{
		cl:        cl,
		namespace: namespace,
	}
}

type CursorStore struct {
	cl        radix.Client
	namespace string
}

func (c CursorStore) GetCursor(ctx context.Context, cursor string) (string, error) {
	var str string
	err := c.cl.Do(ctx, radix.Cmd(&str, "GET", formatKey(c.namespace, cursor)))
	if err != nil {
		return "", errors.Wrap(err, "get cursor")
	}

	return str, nil
}

func (c CursorStore) SetCursor(ctx context.Context, cursor string, value string) error {
	// TODO(corver): Add support for safe sets.

	err := c.cl.Do(ctx, radix.Cmd(nil, "SET", formatKey(c.namespace, cursor), value))
	if err != nil {
		return errors.Wrap(err, "set cursor")
	}

	return nil
}

func (c CursorStore) Flush(ctx context.Context) error {
	// TODO(corver): Add support for async writes.
	return nil
}

func formatKey(namespace, cursor string) string {
	return fmt.Sprintf("reflex_cursors|%s|%s", namespace, cursor)
}
