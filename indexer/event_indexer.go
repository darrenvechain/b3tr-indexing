package indexer

import (
	"context"
	"database/sql"
	"github.com/darrenvechain/thor-go-sdk/client"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/darrenvechain/thor-go-sdk/thorgo/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"log/slog"
	"time"
)

const (
	limit = 256
)

type EventIndexer struct {
	thor        *thorgo.Thor
	db          *sql.DB
	contract    *accounts.Contract
	criteria    []client.EventCriteria
	eventChan   chan []client.EventLog
	blockChan   chan *client.ExpandedBlock
	blocks      *BlockIndexer
	queryRange  uint64
	status      int
	current     uint64
	eventName   string
	processLogs func(events []accounts.Event) error
	logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
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
	processLogs func(events []accounts.Event) error,
) (*EventIndexer, error) {
	contract := thor.Account(contractAddress).Contract(contractABI)
	criteria, err := contract.EventCriteria(eventName)
	if err != nil {
		return nil, err
	}

	if err := createTable(db, createTableSQL); err != nil {
		return nil, err
	}

	blockChan := make(chan *client.ExpandedBlock)
	blockCtx := context.WithValue(ctx, "name", tableName)
	blocks, err := NewBlockIndexer(blockCtx, thor, db, tableName, blockChan)
	if err != nil {
		return nil, err
	}

	indexer := &EventIndexer{
		thor:        thor,
		db:          db,
		contract:    contract,
		criteria:    []client.EventCriteria{criteria},
		eventChan:   make(chan []client.EventLog, 20),
		blockChan:   blockChan,
		blocks:      blocks,
		queryRange:  queryRange,
		status:      Initialised,
		eventName:   eventName,
		processLogs: processLogs,
		logger:      slog.With("name", tableName),
		ctx:         ctx,
	}

	return indexer, nil
}

func createTable(db *sql.DB, createTableSQL string) error {
	_, err := db.Exec(createTableSQL)
	return err
}

func (e *EventIndexer) Status() int {
	return e.status
}

func (e *EventIndexer) Start() error {
	ctx, cancel := context.WithCancel(e.ctx)
	e.cancel = cancel
	e.startCore(ctx)
	latest, err := e.blocks.latest()
	if err != nil {
		return err
	}
	e.current = latest.number + 1

	for {
		select {
		case <-ctx.Done():
			return nil
		case events := <-e.eventChan:
			e.sendLogs(events)
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (e *EventIndexer) startCore(ctx context.Context) {
	go func() {
		e.fastSync(ctx)
		err := e.blocks.Start(e.current)
		if err != nil {
			return
		}
		e.blockSync(ctx)
	}()
}

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
			e.current += e.queryRange
			e.processFastSyncBlock()
		}
	}
}

func (e *EventIndexer) blockSync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			e.logger.Info("stopping block sync", "block", e.blocks.previous.number)
			return
		case block := <-e.blockChan:
			events := e.extractLogsFromBlock(block)
			e.eventChan <- events
		}
	}
}

func (e *EventIndexer) shouldStopFastSync() bool {
	best, err := e.thor.Blocks.Best()
	if err != nil {
		e.logger.Error("failed to get best block", "err", err)
		e.status = Error
		return true
	}

	// for large query range, divide by 2 if it's close to best-180
	if best.Number-180 <= e.current+e.queryRange && e.queryRange > 5 {
		e.queryRange = e.queryRange / 2
		return e.shouldStopFastSync()
	}

	if best.Number-180 <= e.current {
		e.logger.Info("✅  fast sync completed", "block", e.current)
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
}

func (e *EventIndexer) sendLogs(logs []client.EventLog) {
	if len(logs) == 0 {
		return
	}
	decoded, err := e.contract.DecodeEvents(logs)
	if err != nil {
		e.logger.Error("failed to decode events", "err", err)
		return
	}

	if err := e.processLogs(decoded); err != nil {
		e.logger.Error("failed to process log", "err", err)
		return
	}

	e.logger.Info("✅  processed logs", "from", logs[0].Meta.BlockNumber, "to", logs[len(logs)-1].Meta.BlockNumber, "amount", len(logs))
}

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
