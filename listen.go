package pgxlisten

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Listener represents a PostgreSQL notification listener.
type Listener struct {
	channels       []string
	handler        func(payload *pgconn.Notification)
	afterConnect   func(context.Context, *pgx.Conn) error
	onError        func(error)
	reconnectDelay time.Duration
}

// New creates a new Listener with the provided channels and handler.
// It panics if channels is empty or handler is nil.
func New(channels []string, handler func(payload *pgconn.Notification), opts ...Option) *Listener {
	if len(channels) == 0 {
		panic("no channels")
	}

	if handler == nil {
		panic("handler cannot be nil")
	}

	listener := &Listener{
		channels:       channels,
		handler:        handler,
		afterConnect:   nil,
		onError:        nil,
		reconnectDelay: 5 * time.Second,
	}

	for _, opt := range opts {
		opt(listener)
	}

	return listener
}

// Listen starts listening for notifications on the provided channels.
// It will stop listening when the provided context is canceled or an error occurs.
func (l *Listener) Listen(ctx context.Context, conn *pgx.Conn) error {
	for _, channel := range l.channels {
		if _, err := conn.Exec(ctx, "listen "+pgx.Identifier{channel}.Sanitize()); err != nil {
			return fmt.Errorf("listen %q: %w", channel, err)
		}
	}

	if l.afterConnect != nil {
		if err := l.afterConnect(ctx, conn); err != nil {
			return fmt.Errorf("listen after connect: %w", err)
		}
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("waiting for notification: %w", err)
		}

		l.handler(notification)
	}
}

// Run starts listening for notifications on the provided dsn.
// It will reconnect after the provided delay.
// It will stop listening only when the provided context is canceled.
func (l *Listener) Run(ctx context.Context, dsn string) error {
	return l.run(ctx, func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, dsn)
	})
}

// RunFromPool starts listening for notifications on the provided pool connection.
// It will stop listening when the provided context is canceled or an error occurs.
// It will automatically reconnect after the provided delay.
func (l *Listener) RunFromPool(ctx context.Context, pool *pgxpool.Pool) error {
	return l.run(ctx, func(ctx context.Context) (*pgx.Conn, error) {
		c, err := pool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("db pool acquire: %w", err)
		}

		return c.Hijack(), nil
	})
}

// listen is a helper function that calls Listen with the provided connect function and context.
func (l *Listener) listen(ctx context.Context, connect func(context.Context) (*pgx.Conn, error)) error {
	conn, err := connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	return l.Listen(ctx, conn)
}

// run is a helper function that calls run with the provided connect function and context.
// It will automatically reconnect after the provided delay.
func (l *Listener) run(ctx context.Context, connect func(context.Context) (*pgx.Conn, error)) error {
	for {
		if err := l.listen(ctx, connect); err != nil {
			if errors.Is(err, context.Canceled) {
				return context.Cause(ctx)
			}

			if l.onError != nil {
				l.onError(err)
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("db listener done: %w", ctx.Err())
		case <-time.After(l.reconnectDelay): // reconnecting after delay
		}
	}
}
