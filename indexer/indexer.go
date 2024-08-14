package indexer

import (
	"context"
	"database/sql"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
)

type Indexer interface {
	Start()
}

type Func = func(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*EventIndexer, error)
