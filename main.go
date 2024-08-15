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
	indexers, err := createIndexers(ctx, thor, db)
	if err != nil {
		slog.Error("failed to create indexers: %v", "err", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	for _, indexr := range indexers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := indexr.Start()
			if err != nil {
				slog.Error("indexer failed to start", "err", err)
			}
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

func createIndexers(ctx context.Context, thor *thorgo.Thor, db *sql.DB) ([]indexer.Indexer, error) {
	indexers := make([]indexer.Indexer, 0)
	indexerFuncs := []indexer.Func{
		indexer.Vot3Approval,
		indexer.AllocationVoting,
		indexer.Vot3Transfer,
		indexer.B3trTransfer,
		indexer.B3trApproval,
	}

	for _, f := range indexerFuncs {
		indexr, err := f(ctx, thor, db)
		if err != nil {
			return nil, err
		}
		indexers = append(indexers, indexr)
	}

	return indexers, nil
}
