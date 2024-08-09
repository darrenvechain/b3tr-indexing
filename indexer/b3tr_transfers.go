package indexer

import (
	"context"
	"database/sql"
	"github.com/darrenvechain/b3tr-indexing/contracts"
	"github.com/darrenvechain/thor-go-sdk/client"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/darrenvechain/thor-go-sdk/thorgo/accounts"
	"github.com/ethereum/go-ethereum/common"
	"log/slog"
	"math/big"
	"os"
	"time"
)

type B3trTransfer struct {
	Core
	ctx        context.Context
	cancel     context.CancelFunc
	db         *sql.DB
	contract   *accounts.Contract
	eventsChan chan []client.EventLog
	forkChan   chan uint64
}

func NewB3trTransfer(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*B3trTransfer, error) {
	eventChan := make(chan []client.EventLog, 20)
	forkChan := make(chan uint64)
	contract := thor.Account(contracts.B3trAddress).Contract(contracts.B3trABI)
	criteria, err := contract.EventCriteria("Transfer")
	if err != nil {
		return nil, err
	}
	// TODO: remove this
	//db.Exec(`DROP TABLE IF EXISTS b3tr_transfers;`)
	// create the table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS b3tr_transfers (
			id SERIAL PRIMARY KEY,
			"from" BYTEA,
			"to" BYTEA,
			value VARCHAR, 
			block_number NUMERIC,
			block_id BYTEA,
			tx_id BYTEA,
			clause_index NUMERIC
		);
	`)
	if err != nil {
		return nil, err
	}
	base := New(thor, []client.EventCriteria{criteria}, eventChan, forkChan, 500)
	return &B3trTransfer{
		Core:       *base,
		ctx:        ctx,
		db:         db,
		contract:   contract,
		eventsChan: eventChan,
	}, nil
}

func (b *B3trTransfer) Start() {
	b.startCore()

	for {
		select {
		case <-b.ctx.Done():
			b.cancel()
			return
		case events := <-b.eventsChan:
			b.processLogs(events)
		case blockNum := <-b.forkChan:
			b.cancel()
			b.processFork(blockNum)
			b.startCore()
		}
	}
}

func (b *B3trTransfer) startCore() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	lastProcessed := b.lastProcessed()
	b.Core.Start(ctx, lastProcessed)
}

func (b *B3trTransfer) processLogs(logs []client.EventLog) {
	decoded, err := b.contract.DecodeEvents(logs)
	if err != nil {
		slog.Error("failed to decode events: %v", "err", err)
		return
	}

	for i, ev := range decoded {
		from := ev.Args["from"].(common.Address)
		to := ev.Args["to"].(common.Address)
		value := ev.Args["value"].(*big.Int)
		blockNumber := logs[i].Meta.BlockNumber
		blockID := logs[i].Meta.BlockID
		txID := logs[i].Meta.TxID
		clauseIndex := logs[i].Meta.ClauseIndex

		_, err := b.db.Exec(`
						INSERT INTO b3tr_transfers ("from", "to", value, block_number, block_id,tx_id, clause_index)
						VALUES ($1, $2, $3, $4, $5, $6, $7);`,
			from.Bytes(),
			to.Bytes(),
			value.Text(16),
			blockNumber,
			blockID.Bytes(),
			txID.Bytes(),
			clauseIndex,
		)
		if err != nil {
			slog.Error("failed to insert data: %v", "err", err)
			os.Exit(1)
		}
	}

	slog.Info("synced", "from", logs[0].Meta.BlockNumber, "to", logs[len(logs)-1].Meta.BlockNumber, "amount", len(logs))
}

func (b *B3trTransfer) processFork(blockNum uint64) {
	// Function to drain the event channel
	drainChannel := func() {
		for {
			select {
			case events := <-b.eventsChan:
				slog.Warn("draining event channel", "events", len(events))
				// Continue to drain if there are more events
			default:
				// Channel is empty, exit the loop
				return
			}
		}
	}

	// Drain the channel
	drainChannel()

	// delete all rows with block number greater than the forked block
	_, err := b.db.Exec(`DELETE FROM b3tr_transfers WHERE block_number > $1;`, blockNum)
	if err != nil {
		slog.Error("failed to delete rows: %v", "err", err)
	}
}

func (b *B3trTransfer) lastProcessed() Block {
	// get the last row in the table
	row := b.db.QueryRow(`SELECT block_number, block_id
		FROM b3tr_transfers
		ORDER BY block_number DESC
		LIMIT 1;`)

	var blockNumber uint64
	var blockID common.Hash

	err := row.Scan(&blockNumber, &blockID)
	if err != nil {
		slog.Warn("failed to get last processed", "err", err)
		startBlock, err := b.thor.Blocks.ByNumber(18860000)
		if err != nil {
			slog.Error("failed to get start block: %v", "err", err)
			os.Exit(1)
		}
		return Block{startBlock.Number, startBlock.ID, time.Unix(int64(startBlock.Timestamp), 0)}
	}

	return Block{number: blockNumber, id: blockID}
}
