pgx listener
============

```golang
ctx, done := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer done()

const dsn = "postgres://localhost:5432/scc?sslmode=disable"
// create a new listener
listener := pgxlisten.
    NewFromDSN(dsn).
    WithReconnectDelay(time.Second * 10). // delay is the time to wait before reconnecting
    WithOnConnectError(func(err error) {  // the function that runs when a connection error occurs
        fmt.Println("error:", err)
    })

// subscribe to channels and print the received messages to the console in json format
listener.WithChannelHandler(
    "test_channel", // channel name
    // handle is a function that runs when a message is received
    func(payload string) {
        enc := json.NewEncoder(os.Stderr)
        enc.SetIndent("", "  ")
        if err := enc.Encode(json.RawMessage(payload)); err != nil {
            panic(err)
        }
    },
    // onConnect is a function that runs when a new connection is established
    func(_ context.Context, conn *pgx.Conn) {
        rows, err := conn.Query(ctx, "SELECT 1")
        if err != nil {
            panic(err)
        }
        defer rows.Close()
    })

// start listening
if err := listener.Listen(ctx); err != nil {
    panic(err)
}
```