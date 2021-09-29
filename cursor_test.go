package rredis_test

import (
	"testing"
	
	"github.com/corverroos/rredis"
	"github.com/matryer/is"
)

func TestBasicCursor(t *testing.T) {
	ctx, c, ii := setup(t)

	store := rredis.NewCursorStore(c, ns)

	get := func(ii *is.I, expected string) {
		ii.Helper()
		val, err := store.GetCursor(ctx, cursor)
		ii.NoErr(err)
		ii.Equal(val, expected)
	}

	set := func(ii *is.I, val string) {
		ii.Helper()
		ii.NoErr(store.SetCursor(ctx, cursor, val))
	}

	get(ii,"") // No value yet
	set(ii,"a")
	get(ii,"a")
	set(ii,"b")
	set(ii,"c")
	get(ii,"c")
	set(ii,"c")
	get(ii,"c")
	set(ii,"")
	get(ii,"")

	str := randStr()
	set(ii, str)
	get(ii, str)
}