package pgxlisten

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// handler is a internal channel handler with an optional onConnect function.
type handler struct {
	handle    func(string)
	onConnect func(context.Context, *pgx.Conn)
}

// Listener is a PostgreSQL listener.
type Listener struct {
	conn     func(context.Context) (*pgx.Conn, error) // conn is a function that returns a new connection
	delay    time.Duration                            // delay is the time to wait before reconnecting
	onError  func(error)                              // onError is a function that runs when an error occurs
	handlers map[string]handler                       // handlers is a map of channel names to handlers
}

// New creates a new PostgreSQL listener.
//
// init is a function that returns a new connection for the exclusive use of the Listener.
// Listener takes responsibility for closing any connection it receives.
func New(init func(context.Context) (*pgx.Conn, error)) *Listener {
	if init == nil {
		panic("connector cannot be nil")
	}

	return &Listener{
		conn:     init,
		delay:    time.Minute,
		handlers: make(map[string]handler),
		onError:  nil,
	}
}

// NewFromDSN creates a new PostgreSQL listener from a connection string.
func NewFromDSN(connString string) *Listener {
	return New(func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, connString)
	})
}

// NewFromPool creates a new PostgreSQL listener from a pool of connections.
func NewFromPool(pool *pgxpool.Pool) *Listener {
	return New(func(ctx context.Context) (*pgx.Conn, error) {
		c, err := pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		return c.Hijack(), nil
	})
}

// WithReconnectDelay sets the delay between reconnects. Defaults to 1 minute.
func (l *Listener) WithReconnectDelay(delay time.Duration) *Listener {
	l.delay = delay

	return l
}

// WithOnConnectError sets the error handler function that runs when an connection error occurs.
func (l *Listener) WithOnConnectError(onError func(error)) *Listener {
	l.onError = onError

	return l
}

// WithChannelHandler adds a channel handler.
//
// The handler will be called with the payload when the named channel receives data from PostgreSQL notifications.
// If onConnect is provided, it will be called when the connection is established.
//
// The name cannot be an empty string and the handler cannot be nil.
// If a handler already exists for the name, it will panic.
func (l *Listener) WithChannelHandler(
	name string,
	handle func(payload string),
	onConnect ...func(context.Context, *pgx.Conn),
) *Listener {
	name = strings.TrimSpace(name)

	if name == "" {
		panic("channel name cannot be empty")
	}

	if handle == nil {
		panic("handler cannot be nil")
	}

	var on func(context.Context, *pgx.Conn)
	if l := len(onConnect); l > 0 {
		if l > 1 {
			panic("too many onConnect handlers")
		}

		on = onConnect[0]
	}

	if l.handlers == nil {
		l.handlers = make(map[string]handler)
	} else if _, ok := l.handlers[name]; ok {
		panic("handler already exists")
	}

	l.handlers[name] = handler{
		handle:    handle,
		onConnect: on,
	}

	return l
}

// Listen listens for notifications and calls the registered handlers.
// It blocks until the context is canceled.
//
// Automatically reconnects if an error occurs.
// If an error occurs and onError is set, it will be called.
// The delay between reconnects is determined by WithReconnectDelay.
func (l *Listener) Listen(ctx context.Context) error {
	if len(l.handlers) == 0 {
		return fmt.Errorf("no handlers")
	}

	for {
		err := l.listen(ctx)
		if err != nil && l.onError != nil {
			l.onError(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.delay):
			// reconnecting after delay
		}
	}
}

func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.conn(ctx)
	if err != nil {
		return fmt.Errorf("connection: %w", err)
	}
	defer conn.Close(context.Background()) //nolint:contextcheck // it's okay to close the connection

	for channel, handler := range l.handlers {
		_, err := conn.Exec(ctx, "listen "+pgx.Identifier{channel}.Sanitize())
		if err != nil {
			return fmt.Errorf("listen %q: %w", channel, err)
		}

		if handler.onConnect != nil {
			handler.onConnect(ctx, conn)
		}
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("waiting for notification: %w", err)
		}

		if handler := l.handlers[notification.Channel]; handler.handle != nil {
			handler.handle(notification.Payload)
		}
	}
}
