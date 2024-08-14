package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/darrenvechain/b3tr-indexing/queue"
	"github.com/darrenvechain/thor-go-sdk/client"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/darrenvechain/thor-go-sdk/thorgo/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"log/slog"
	"os"
	"time"
)

// Constants
const (
	Initialised = iota
	Syncing
	Synced
	Error

	limit = 256
)

func Status(status int) string {
	switch status {
	case Initialised:
		return "Initialised"
	case Syncing:
		return "Syncing"
	case Synced:
		return "Synced"
	case Error:
		return "Error"
	default:
		return "Unknown"
	}
}

// Types
type Block struct {
	Number uint64
	ID     common.Hash
	Time   time.Time
}

type EventIndexer struct {
	thor       *thorgo.Thor
	db         *sql.DB
	contract   *accounts.Contract
	criteria   []client.EventCriteria
	eventChan  chan []client.EventLog
	forkChan   chan uint64
	queryRange uint64
	status     int
	blocks     *queue.Queue[Block]
	current    uint64
	tableName  string
	eventName  string
	processLog func(ev accounts.Event) error
	logger     *slog.Logger
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewEventIndexer(
	ctx context.Context,
	thor *thorgo.Thor,
	db *sql.DB,
	contractAddress common.Address,
	contractABI abi.ABI,
	eventName string,
	queryRange uint64,
	tableName string,
	createTableSQL string,
	processLog func(ev accounts.Event) error,
) (*EventIndexer, error) {
	contract := thor.Account(contractAddress).Contract(contractABI)
	criteria, err := contract.EventCriteria(eventName)
	if err != nil {
		return nil, err
	}

	if err := createTable(db, createTableSQL); err != nil {
		return nil, err
	}

	return &EventIndexer{
		thor:       thor,
		db:         db,
		contract:   contract,
		criteria:   []client.EventCriteria{criteria},
		eventChan:  make(chan []client.EventLog, 20),
		forkChan:   make(chan uint64),
		queryRange: queryRange,
		status:     Initialised,
		blocks:     queue.New[Block](360),
		tableName:  tableName,
		eventName:  eventName,
		processLog: processLog,
		logger:     slog.With("name", tableName),
		ctx:        ctx,
	}, nil
}

func createTable(db *sql.DB, createTableSQL string) error {
	_, err := db.Exec(createTableSQL)
	return err
}

// Main methods
func (e *EventIndexer) Status() int {
	return e.status
}

func (e *EventIndexer) Start() {
	ctx, cancel := context.WithCancel(e.ctx)
	e.cancel = cancel
	lastProcessed := e.lastProcessed()
	e.startCore(ctx, lastProcessed)

	for {
		select {
		case <-ctx.Done():
			return
		case events := <-e.eventChan:
			e.processLogs(events)
		case blockNum := <-e.forkChan:
			e.handleFork(blockNum)
		}
	}
}

func (e *EventIndexer) handleFork(blockNum uint64) {
	e.cancel()
	e.processFork(blockNum)
	e.Start() // Restart the indexer
}

func (e *EventIndexer) startCore(ctx context.Context, block Block) {
	go func() {
		e.current = block.Number
		e.status = Syncing
		e.fastSync(ctx)
		e.sync(ctx)
	}()
}

// Sync methods
func (e *EventIndexer) fastSync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			e.logger.Info("stopping fast sync", "block", e.current)
			return
		default:
			if e.shouldStopFastSync() {
				return
			}
			e.processFastSyncBlock()
		}
	}
}

func (e *EventIndexer) shouldStopFastSync() bool {
	endBlock, err := e.thor.Blocks.Best()
	if err != nil {
		e.logger.Error("failed to get best block", "err", err)
		e.status = Error
		return true
	}

	if e.current >= endBlock.Number-e.queryRange {
		e.logger.Info("fast sync complete", "block", e.current)
		return true
	}

	return false
}

func (e *EventIndexer) processFastSyncBlock() {
	events, err := e.fetchEvents(e.current+1, e.current+e.queryRange)
	if err != nil {
		e.logger.Error("failed to fetch events", "err", err)
		e.status = Error
		time.Sleep(10 * time.Second)
		return
	}

	if len(events) > 0 {
		e.eventChan <- events
	}

	e.logger.Info("🚀 fast sync processed", "block", e.current, "range", e.queryRange, "amount", len(events))

	e.current += e.queryRange
}

func (e *EventIndexer) sync(ctx context.Context) {
	e.logger.Info("starting regular sync", "block", e.current)

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("stopping regular sync", "block", e.current)
			return
		default:
			e.processSyncBlock()
		}
	}
}

func (e *EventIndexer) processSyncBlock() {
	currentBlock, events, err := e.fetchBlockEvents(e.current + 1)
	if err != nil {
		e.handleBlockError(err)
		return
	}

	if len(events) > 0 {
		e.eventChan <- events
	}

	e.blocks.Enqueue(Block{
		Number: currentBlock.Number,
		ID:     currentBlock.ID,
		Time:   time.Unix(int64(currentBlock.Timestamp), 0).UTC(),
	})

	e.logger.Info("📦 new block processed", "block", e.current, "status", Status(e.status))

	nextBlockTime := time.Unix(int64(currentBlock.Timestamp), 0).
		UTC().
		Add(10 * time.Second)

	now := time.Now().UTC()
	if now.Before(nextBlockTime) {
		e.status = Synced
		time.Sleep(nextBlockTime.Add(200 * time.Millisecond).Sub(now))
	} else {
		e.status = Syncing
	}

	e.current++
}

// Event processing methods
func (e *EventIndexer) processLogs(logs []client.EventLog) {
	decoded, err := e.contract.DecodeEvents(logs)
	if err != nil {
		e.logger.Error("failed to decode events", "err", err)
		return
	}

	for _, ev := range decoded {
		if err := e.processLog(ev); err != nil {
			e.logger.Error("failed to process log", "err", err)
			return
		}
	}

	e.logger.Info("✅  processed logs", "from", logs[0].Meta.BlockNumber, "to", logs[len(logs)-1].Meta.BlockNumber, "amount", len(logs))
}

// Error handling methods
func (e *EventIndexer) handleBlockError(err error) {
	if errors.Is(err, client.ErrNotFound) {
		e.handleBlockNotFound()
		return
	}

	e.logger.Error("failed to fetch block events", "block", e.current, "err", err)
	e.status = Error
	time.Sleep(10 * time.Second)
}

func (e *EventIndexer) handleBlockNotFound() {
	previous, ok := e.blocks.Newest()
	if ok {
		predictedTime := previous.Time.Add(10*time.Second + 200*time.Millisecond)
		time.Sleep(time.Until(predictedTime))
	} else {
		time.Sleep(10 * time.Second)
	}
}

// Utility methods
func (e *EventIndexer) fetchEvents(start, end uint64) ([]client.EventLog, error) {
	allEvents := make([]client.EventLog, 0)
	offset := uint64(0)
	for {
		events, err := e.thor.Events(e.criteria).
			Ascending().
			BlockRange(start, end).
			Apply(offset, limit)

		if err != nil {
			return nil, err
		}

		allEvents = append(allEvents, events...)

		if len(events) == 0 || len(events) < limit {
			break
		}

		offset += limit
	}

	return allEvents, nil
}

func (e *EventIndexer) fetchBlockEvents(number uint64) (*client.ExpandedBlock, []client.EventLog, error) {
	block, err := e.thor.Blocks.Expanded(fmt.Sprintf("%d", number))
	if err != nil {
		return nil, nil, err
	}

	if e.isForkedBlock(block) {
		return nil, nil, fmt.Errorf("block %d has a different parent", number)
	}

	logs := e.extractLogsFromBlock(block)
	return block, logs, nil
}

func (e *EventIndexer) isForkedBlock(block *client.ExpandedBlock) bool {
	previous, ok := e.blocks.Newest()
	if ok && block.ParentID != previous.ID {
		e.detectForkedBlock(block)
		return true
	}
	return false
}

func (e *EventIndexer) extractLogsFromBlock(block *client.ExpandedBlock) []client.EventLog {
	logs := make([]client.EventLog, 0)
	for _, tx := range block.Transactions {
		for clauseIndex, output := range tx.Outputs {
			for _, event := range output.Events {
				if e.matchesCriteria(event) {
					logs = append(logs, e.createEventLog(event, block, tx, clauseIndex))
				}
			}
		}
	}
	return logs
}

func (e *EventIndexer) createEventLog(event client.Event, block *client.ExpandedBlock, tx client.BlockTransaction, clauseIndex int) client.EventLog {
	return client.EventLog{
		Address: &event.Address,
		Topics:  event.Topics,
		Data:    event.Data,
		Meta: client.LogMeta{
			BlockID:     block.ID,
			BlockNumber: block.Number,
			BlockTime:   block.Timestamp,
			TxID:        tx.ID,
			TxOrigin:    tx.Origin,
			ClauseIndex: uint64(clauseIndex),
		},
	}
}

func (e *EventIndexer) matchesCriteria(event client.Event) bool {
	for _, criteria := range e.criteria {
		if e.eventMatchesCriteria(event, criteria) {
			return true
		}
	}
	return false
}

func (e *EventIndexer) eventMatchesCriteria(event client.Event, criteria client.EventCriteria) bool {
	if criteria.Address != nil && *criteria.Address != event.Address {
		return false
	}

	topics := []*common.Hash{criteria.Topic0, criteria.Topic1, criteria.Topic2, criteria.Topic3, criteria.Topic4}
	for i, topic := range topics {
		if topic != nil && *topic != event.Topics[i] {
			return false
		}
	}

	return true
}

func (e *EventIndexer) detectForkedBlock(block *client.ExpandedBlock) {
	for {
		if e.blocks.Len() == 0 {
			e.forkChan <- block.Number
			return
		}

		previousInMem, _ := e.blocks.Newest()
		previousOnChain, err := e.thor.Blocks.ByNumber(previousInMem.Number)
		if err != nil {
			e.logger.Error("failed to get block", "number", previousInMem.Number, "err", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if previousOnChain.ID == block.ParentID {
			e.forkChan <- previousInMem.Number
			return
		}

		e.blocks.Pop()
	}
}

func (e *EventIndexer) processFork(blockNum uint64) {
	e.drainEventChannel()
	e.deleteRowsAfterBlock(blockNum)
}

func (e *EventIndexer) drainEventChannel() {
	for {
		select {
		case events := <-e.eventChan:
			e.logger.Warn("draining event channel", "events", len(events))
		default:
			return
		}
	}
}

func (e *EventIndexer) deleteRowsAfterBlock(blockNum uint64) {
	_, err := e.db.Exec("DELETE FROM "+e.tableName+" WHERE block_number > $1;", blockNum)
	if err != nil {
		e.logger.Error("failed to delete rows", "err", err)
	}
}

func (e *EventIndexer) lastProcessed() Block {
	query := "SELECT block_number, block_id FROM " + e.tableName + " ORDER BY block_number DESC LIMIT 1;"
	row := e.db.QueryRow(query)

	var blockNumber uint64
	var blockID common.Hash

	err := row.Scan(&blockNumber, &blockID)
	if err != nil {
		e.logger.Warn("failed to get last processed", "err", err)
		return e.getStartBlock()
	}

	return Block{Number: blockNumber, ID: blockID}
}

func (e *EventIndexer) getStartBlock() Block {
	startBlock, err := e.thor.Blocks.ByNumber(18860000)
	if err != nil {
		e.logger.Error("failed to get start block", "err", err)
		os.Exit(1)
	}
	return Block{Number: startBlock.Number, ID: startBlock.ID, Time: time.Unix(int64(startBlock.Timestamp), 0)}
}
