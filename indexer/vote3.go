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

func Vot3Approval(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*EventIndexer, error) {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS vot3_approvals (
			id SERIAL PRIMARY KEY,
			"owner" BYTEA,
			"spender" BYTEA,
			value VARCHAR, 
			block_number NUMERIC,
			block_id BYTEA,
			tx_id BYTEA,
			clause_index NUMERIC
		);
	`

	processLogs := func(events []accounts.Event) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		stmt, err := tx.Prepare(`
			INSERT INTO vot3_approvals ("owner", "spender", value, block_number, block_id, tx_id, clause_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, ev := range events {
			owner := ev.Args["owner"].(common.Address)
			spender := ev.Args["spender"].(common.Address)
			value := ev.Args["value"].(*big.Int)

			_, err := stmt.Exec(
				owner.Bytes(),
				spender.Bytes(),
				value.Text(16),
				ev.Log.Meta.BlockNumber,
				ev.Log.Meta.BlockID.Bytes(),
				ev.Log.Meta.TxID.Bytes(),
				ev.Log.Meta.ClauseIndex,
			)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	}

	return NewEventIndexer(
		ctx,
		thor,
		db,
		contracts.Vot3Address,
		contracts.Vot3ABI,
		"Approval",
		25000,
		"vot3_approvals",
		createTableSQL,
		processLogs,
	)
}

func Vot3Transfer(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*EventIndexer, error) {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS vot3_transfers (
			id SERIAL PRIMARY KEY,
			"from" BYTEA,
			"to" BYTEA,
			value VARCHAR, 
			block_number NUMERIC,
			block_id BYTEA,
			tx_id BYTEA,
			clause_index NUMERIC
		);
	`

	processLogs := func(events []accounts.Event) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		stmt, err := tx.Prepare(`
			INSERT INTO vot3_transfers ("from", "to", value, block_number, block_id, tx_id, clause_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, ev := range events {
			from := ev.Args["from"].(common.Address)
			to := ev.Args["to"].(common.Address)
			value := ev.Args["value"].(*big.Int)

			_, err := stmt.Exec(
				from.Bytes(),
				to.Bytes(),
				value.Text(16),
				ev.Log.Meta.BlockNumber,
				ev.Log.Meta.BlockID.Bytes(),
				ev.Log.Meta.TxID.Bytes(),
				ev.Log.Meta.ClauseIndex,
			)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	}

	return NewEventIndexer(
		ctx,
		thor,
		db,
		contracts.Vot3Address,
		contracts.Vot3ABI,
		"Transfer",
		2500,
		"vot3_transfers",
		createTableSQL,
		processLogs,
	)
}
