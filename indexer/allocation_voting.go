package indexer

import (
	"context"
	"database/sql"
	"github.com/darrenvechain/b3tr-indexing/contracts"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/darrenvechain/thor-go-sdk/thorgo/accounts"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

func AllocationVoting(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*EventIndexer, error) {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS allocation_votes (
			id SERIAL PRIMARY KEY,
			"voter" BYTEA,
			"round_id" BYTEA,
			"apps_ids" TEXT,
			"vote_weights" TEXT,
			block_number NUMERIC,
			block_id BYTEA,
			tx_id BYTEA,
			clause_index NUMERIC
		);
	`

	processLog := func(ev accounts.Event) error {
		voter := ev.Args["voter"].(common.Address)
		roundID := ev.Args["roundId"].(*big.Int)
		appsIds := ev.Args["appsIds"].([][32]uint8)
		voteWeights := ev.Args["voteWeights"].([]*big.Int)

		for i, v := range appsIds {
			_, err := db.Exec(`
			INSERT INTO allocation_votes ("voter", "round_id", "apps_ids", "vote_weights", block_number, block_id, tx_id, clause_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`,
				voter.Bytes(),
				roundID.Bytes(),
				common.Bytes2Hex(v[:]),
				voteWeights[i].Text(16),
				ev.Log.Meta.BlockNumber,
				ev.Log.Meta.BlockID.Bytes(),
				ev.Log.Meta.TxID.Bytes(),
				ev.Log.Meta.ClauseIndex,
			)

			if err != nil {
				return err
			}
		}

		return nil
	}

	return NewEventIndexer(
		ctx,
		thor,
		db,
		contracts.XAllocationVotingAddress,
		contracts.XAllocationVotingABI,
		"AllocationVoteCast",
		1000,
		"allocation_votes",
		createTableSQL,
		processLog,
	)
}
