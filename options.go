package pgxlisten

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Option represents an option for the Listen function.
type Option func(*Listener)

// WithAfterConnect adds a function that will be called after the connection is established.
// It can be used to perform additional setup.
// If it returns an error, the connection will be closed and listening will be stopped.
func WithAfterConnect(afterConnect func(context.Context, *pgx.Conn) error) Option {
	return func(l *Listener) {
		l.afterConnect = afterConnect
	}
}

// WithOnError add a function that will be called when a connection error occurs during listening.
// It can be used to handle connection errors during listening.
func WithOnError(onError func(error)) Option {
	return func(l *Listener) {
		l.onError = onError
	}
}

// WithReconnectDelay sets the delay between reconnect attempts.
// The default delay is 5 seconds.
func WithReconnectDelay(delay time.Duration) Option {
	return func(l *Listener) {
		l.reconnectDelay = delay
	}
}
