package pgxlisten_test

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mdigger/pgxlisten"
)

func Example() {
	// create a new listener
	listen := pgxlisten.New(
		// channel names to listen on
		[]string{"listen1", "listen2", "listen3"},
		// notification handler function
		func(payload *pgconn.Notification) {
			slog.Info("notification",
				slog.String("channel", payload.Channel),
				slog.String("payload", string(payload.Payload)))
		},
		// after connect logger
		pgxlisten.WithAfterConnect(func(_ context.Context, conn *pgx.Conn) error {
			slog.Info("listener connected")
			return nil
		}),
		// connection error logger
		pgxlisten.WithOnError(func(err error) {
			slog.Error("listener", slog.String("error", err.Error()))
		}),
		// reconnect delay (default 5 sec)
		pgxlisten.WithReconnectDelay(time.Second),
	)

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer done()

	const dsn = "postgres://localhost:5432/test?sslmode=disable"

	// connect and start listening; auto restart on error
	if err := listen.Run(ctx, dsn); err != nil {
		slog.Error("listen", slog.String("error", err.Error()))
	}
}
