package indexer

import (
	"context"
	"errors"
	"fmt"
	"github.com/darrenvechain/b3tr-indexing/queue"
	"github.com/darrenvechain/thor-go-sdk/client"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/ethereum/go-ethereum/common"
	"log/slog"
	"time"
)

const (
	Initialised = iota
	Syncing
	Synced
	Error
)

type Indexer interface {
	Start()
}

type Block struct {
	number uint64
	id     common.Hash
	time   time.Time
}

type Core struct {
	thor       *thorgo.Thor
	criteria   []client.EventCriteria
	eventChan  chan []client.EventLog
	forkChan   chan uint64
	queryRange uint64
	parentID   common.Hash
	status     int
	blocks     *queue.Queue[Block]
	current    uint64
}

func New(
	thor *thorgo.Thor,
	criteria []client.EventCriteria,
	eventChan chan []client.EventLog,
	forkChan chan uint64,
	queryRange uint64,
) *Core {
	blockchain := queue.New[Block](360)
	return &Core{
		thor:       thor,
		criteria:   criteria,
		eventChan:  eventChan,
		forkChan:   forkChan,
		status:     Initialised,
		queryRange: queryRange,
		blocks:     blockchain,
	}
}

func (i *Core) Status() int {
	return i.status
}

func (i *Core) Start(ctx context.Context, block Block) {
	go func() {
		i.current = block.number
		i.status = Syncing
		i.fastSync(ctx)
		i.sync(ctx)
	}()
}

// fastSync repeatedly queries all events in a range of QueryRange blocks
func (i *Core) fastSync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping fast sync", "block", i.current)
			return
		default:
			endBlock, err := i.thor.Blocks.Best()
			if err != nil {
				slog.Error("failed to get best block", "err", err)
				i.status = Error
				return
			}

			if i.current >= endBlock.Number-i.queryRange {
				slog.Info("fast sync complete", "block", i.current)
				return
			}

			events, err := i.fetchEvents(i.current+1, i.current+i.queryRange)
			if err != nil {
				slog.Error("failed to fetch events", "err", err)
				i.status = Error
				time.Sleep(10 * time.Second)
				continue
			}

			if len(events) > 0 {
				i.eventChan <- events
			}

			i.current += i.queryRange
		}
	}
}

// sync queries events block by block, allowing for real-time indexing and chain fork rollbacks
func (i *Core) sync(ctx context.Context) {
	slog.Info("starting regular sync", "block", i.current)

	for {
		select {
		case <-ctx.Done():
			slog.Info("stopping regular sync", "block", i.current)
			return
		default:
			next, events, err := i.fetchBlockEvents(i.current + 1)
			if err != nil {
				i.handleBlockError(err)
				continue
			}

			if len(events) > 0 {
				i.eventChan <- events
			}

			i.blocks.Enqueue(Block{
				number: next.Number,
				id:     next.ID,
				time:   time.Unix(int64(next.Timestamp), 0),
			})

			i.current++
		}
	}
}

func (i *Core) handleBlockError(err error) {
	previous, ok := i.blocks.Newest()

	if errors.Is(err, client.ErrNotFound) {
		if ok {
			// sleep until the previous block time + 10 seconds
			predictedTime := previous.time.Add(10*time.Second + 200*time.Millisecond)
			time.Sleep(time.Until(predictedTime))
		} else {
			time.Sleep(10 * time.Second)
		}
		return
	}

	slog.Error("failed to fetch block events", "block", i.current, "err", err)
	i.status = Error
	time.Sleep(10 * time.Second)
}

const limit = 256

// fetchEvents queries events for a given block range
func (i *Core) fetchEvents(start, end uint64) ([]client.EventLog, error) {
	allEvents := make([]client.EventLog, 0)
	offset := uint64(0)
	for {
		events, err := i.thor.Events(i.criteria).
			Ascending().
			BlockRange(start, end).
			Apply(offset, limit)

		if err != nil {
			return nil, err
		}

		allEvents = append(allEvents, events...)

		if len(events) == 0 || len(events) < 256 {
			break
		}

		offset += limit
	}

	return allEvents, nil
}

func (i *Core) fetchBlockEvents(number uint64) (*client.ExpandedBlock, []client.EventLog, error) {
	block, err := i.thor.Blocks.Expanded(fmt.Sprintf("%d", number))
	if err != nil {
		return nil, nil, err
	}
	previous, ok := i.blocks.Newest()
	if ok {
		if block.ParentID != previous.id {
			i.detectForkedBlock(block)
			return nil, nil, fmt.Errorf("block %d has a different parent", number)
		}
	}

	logs := make([]client.EventLog, 0)
	for _, tx := range block.Transactions {
		for clauseIndex, output := range tx.Outputs {
			for _, event := range output.Events {
				if !i.matchesCriteria(event) {
					continue
				}
				logs = append(logs, client.EventLog{
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
				})
			}
		}
	}

	return block, logs, nil
}

func (i *Core) matchesCriteria(event client.Event) bool {
	for _, criteria := range i.criteria {
		if criteria.Address != nil && *criteria.Address != event.Address {
			continue
		}

		if criteria.Topic0 != nil && *criteria.Topic0 != event.Topics[0] {
			continue
		}

		if criteria.Topic1 != nil && *criteria.Topic1 != event.Topics[1] {
			continue
		}

		if criteria.Topic2 != nil && *criteria.Topic2 != event.Topics[2] {
			continue
		}

		if criteria.Topic3 != nil && *criteria.Topic3 != event.Topics[3] {
			continue
		}

		if criteria.Topic4 != nil && *criteria.Topic4 != event.Topics[4] {
			continue
		}

		return true
	}

	return false
}

func (i *Core) detectForkedBlock(block *client.ExpandedBlock) {
	// get block -1 from the chain until we find a common parent in the queue
	for {
		if i.blocks.Len() == 0 {
			i.forkChan <- block.Number
			return
		}

		previousInMem, _ := i.blocks.Newest()
		previousOnChain, err := i.thor.Blocks.ByNumber(previousInMem.number)
		if err != nil {
			slog.Error("failed to get block", "number", previousInMem.number, "err", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if previousOnChain.ID == block.ParentID {
			i.forkChan <- previousInMem.number
			return
		}

		i.blocks.Pop()
	}
}
