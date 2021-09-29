# rredis

A [reflex](https://github.com/luno/reflex) stream client for a [redis streams](https://redis.io/topics/streams-intro) 
using the [radix](https://github.com/mediocregopher/radix) client implementation.

It provides an API for inserting data into a stream and for consuming data from a stream with at-least-once semantics.
It also provides a reflex.CursorStore implementation for storing cursors in redis.

### Usage

```
// Define your consumer business logic
fn := func(ctx context.Context, fate fate.Fate, e *reflex.Event) error {
  fmt.Print("Consuming redis stream event", e)
  return fate.Tempt() // Fate injects application errors at runtime, enforcing idempotent logic.
}

// Define some more variables
var namespace, streamName, redisAddr, consumerName string
var ctx context.Context  

// Connect to redis
con, _ := radix.Dial(ctx, "tcp", redisAddr)

// Setup rredis and reflex
stream := rredis.NewStream(con, namespace, stream)
cstore := rredis.NewCursorStore(con, namespace)

consumer := reflex.NewConsumer(consumerName, fn)
spec := reflex.NewSpec(stream.Stream, cstore, consumer)

// Insert some data concurrently
go func() {
  for {
    _ = stream.Insert(ctx, []byte(fmt.Sprint(time.Now())))
    time.Sleep(time.Second)
  }
}()

// Stream forever!
// Progress is stored in the cursor store, so restarts or any error continue where it left off.
for {
  err := reflex.Run(context.Backend(), spec)
  if err != nil { // Note Run always returns non-nil error
    log.Printf("stream error: %v", err)
  }
}
```

## Notes

- Since reflex events have specific fields (type, foreignID, timestamp, data) the redis stream entries need to adhere to a specific format. It is therefore advised to use Stream.Insert to ensure the correct format.
- At-least-once semantics are also provided by redis consumer groups, but it doesn't provide strict ordering. rredis maintains strict ordering and can provide sharding using reflex/rpatterns.Parralel.