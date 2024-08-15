package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/darrenvechain/thor-go-sdk/client"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/ethereum/go-ethereum/common"
	"log/slog"
	"time"
)

type Block struct {
	number uint64
	id     common.Hash
	time   time.Time
}

type BlockIndexer struct {
	ctx       context.Context
	thor      *thorgo.Thor
	db        *sql.DB
	tableName string
	blockChan chan *client.ExpandedBlock
	previous  *Block
	start     uint64
	logger    *slog.Logger
	cancel    context.CancelFunc
	status    int
}

func NewBlockIndexer(ctx context.Context, thor *thorgo.Thor, db *sql.DB, tableName string, blockChan chan *client.ExpandedBlock) (*BlockIndexer, error) {
	logger := slog.With("name", ctx.Value("name"))

	return &BlockIndexer{
		ctx:       ctx,
		thor:      thor,
		db:        db,
		tableName: tableName,
		blockChan: blockChan,
		logger:    logger,
		status:    Initialised,
	}, nil
}

func (b *BlockIndexer) Start(start uint64) error {
	next, err := b.thor.Blocks.Expanded(fmt.Sprintf("%d", start))
	if err != nil {
		return err
	}
	b.previous = &Block{
		number: next.Number - 1,
		id:     next.ParentID,
		time:   time.Unix(int64(next.Timestamp-10), 0).UTC(),
	}
	go func() {
		ctx, cancel := context.WithCancel(b.ctx)
		b.cancel = cancel
		b.syncBlocks(ctx)
	}()
	return nil
}

func (b *BlockIndexer) syncBlocks(ctx context.Context) {
	b.logger.Info("starting block sync", "block", b.previous.number+1)
	b.status = Syncing

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("stopping regular sync", "block", b.previous.number)
			return
		default:
			b.processSyncBlock()
		}
	}
}

func (b *BlockIndexer) processSyncBlock() {
	next := b.previous.number + 1
	nextBlock, err := b.thor.Blocks.Expanded(fmt.Sprintf("%d", next))
	if err != nil {
		b.handleBlockError(err, next)
		return
	}

	if b.previous.id != nextBlock.ParentID {
		b.logger.Info("fork detected", "block", nextBlock.Number)
		if err := b.handleFork(); err != nil {
			b.logger.Error("failed to handle fork", "err", err)
			return
		}
	}

	b.blockChan <- nextBlock
	b.previous = &Block{
		number: nextBlock.Number,
		id:     nextBlock.ID,
		time:   time.Unix(int64(nextBlock.Timestamp), 0).UTC(),
	}

	b.logger.Info("📦 new block processed", "block", b.previous.number, "status", Status(b.status))

	// if the time of the block is in the last 10 seconds, predict next block time and sleep until then
	nextBlockTime := b.previous.time.Add(10 * time.Second)
	now := time.Now().UTC()
	if now.Before(nextBlockTime) {
		b.status = Synced
		time.Sleep(nextBlockTime.Add(200 * time.Millisecond).Sub(now))
	} else {
		b.status = Syncing
	}
}

func (b *BlockIndexer) handleBlockError(err error, number uint64) {
	if errors.Is(err, client.ErrNotFound) {
		time.Sleep(b.previous.time.Add(10 * time.Second).Sub(time.Now().UTC()))
		return
	}

	b.logger.Error("unexpected error retrieving block", "err", err, "block", number)
	b.status = Error
	time.Sleep(10 * time.Second)
}

func (b *BlockIndexer) handleFork() error {
	b.cancel()
	b.drainEventChannel()

	time.Sleep(10 * time.Second)

	// keep getting the latest from the DB Table until the block number + ID matches that on chain
	localBlock, err := b.latest()
	if err != nil {
		return err
	}
	chainBlock, err := b.thor.Blocks.ByNumber(localBlock.number)
	if err != nil {
		return err
	}

	if localBlock.id == chainBlock.ID {
		b.logger.Info("fork resolved", "block", localBlock.number)
		b.previous = localBlock
		return b.Start(localBlock.number + 1)
	}

	b.logger.Info("fork not resolved", "block", localBlock.number, "local", localBlock.id.Hex(), "chain", chainBlock.ID.Hex())
	b.removeRows(localBlock.number)

	return b.handleFork()
}

func (b *BlockIndexer) drainEventChannel() {
	for {
		select {
		case <-b.blockChan:
			b.logger.Warn("draining block chan")
		default:
			return
		}
	}
}

func (b *BlockIndexer) latest() (*Block, error) {
	query := "SELECT block_number, block_id FROM " + b.tableName + " ORDER BY block_number DESC LIMIT 1;"
	row := b.db.QueryRow(query)

	var blockNumber uint64
	var blockID common.Hash

	err := row.Scan(&blockNumber, &blockID)
	if err != nil {
		b.logger.Warn("failed to get last processed", "err", err)
		previousBlock, err := b.thor.Blocks.ByNumber(18860000)
		if err != nil {
			return nil, err
		}
		return &Block{number: previousBlock.Number, id: previousBlock.ID}, nil
	}

	return &Block{number: blockNumber, id: blockID}, nil
}

func (b *BlockIndexer) removeRows(blockNum uint64) {
	_, err := b.db.Exec("DELETE FROM "+b.tableName+" WHERE block_number >= $1;", blockNum)
	if err != nil {
		b.logger.Error("failed to delete rows", "err", err)
	}
}
