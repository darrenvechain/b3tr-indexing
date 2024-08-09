package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/darrenvechain/b3tr-indexing/indexer"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	_ "github.com/lib/pq"
	"github.com/lmittmann/tint"
)

func init() {
	w := os.Stderr
	slog.SetDefault(slog.New(
		tint.NewHandler(w, &tint.Options{
			AddSource:  true,
			Level:      slog.LevelDebug,
			TimeFormat: time.TimeOnly,
		}),
	))
}

// TODO: use a flag
var thorURL = "https://mainnet.vechain.org"

// TODO: use a flag
var postgresURI = "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable"

func main() {
	thor, err := thorgo.FromURL(thorURL)
	if err != nil {
		slog.Error("failed to create thor client: %v", "err", err)
		os.Exit(1)
	}

	db, err := sql.Open("postgres", postgresURI)
	if err != nil {
		slog.Error("failed to create database: %v", "err", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("closing database")
		if err := db.Close(); err != nil {
			slog.Error("failed to close database: %v", "err", err)
		}
	}()

	ctx := createExitContext()
	indexers := createIndexers(ctx, thor, db)

	var wg sync.WaitGroup
	for _, indexr := range indexers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			indexr.Start()
		}()
	}

	wg.Wait()
	slog.Info("all indexers stopped")
}

func createExitContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		exitSignalCh := make(chan os.Signal, 1)
		signal.Notify(exitSignalCh, os.Interrupt, syscall.SIGTERM)

		sig := <-exitSignalCh
		slog.Info("exit signal received", "signal", sig)
		cancel()
	}()
	return ctx
}

func createIndexers(ctx context.Context, thor *thorgo.Thor, db *sql.DB) []indexer.Indexer {
	indexers := make([]indexer.Indexer, 0)

	if true {
		b3trTransfer, err := indexer.NewB3trTransfer(ctx, thor, db)
		if err != nil {
			slog.Error("failed to create b3tr transfer indexer: %v", "err", err)
			os.Exit(1)
		}
		indexers = append(indexers, b3trTransfer)
	}

	return indexers
}
