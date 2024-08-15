package indexer

import (
	"context"
	"database/sql"
	"github.com/darrenvechain/b3tr-indexing/contracts"
	"github.com/darrenvechain/thor-go-sdk/client"
	"github.com/darrenvechain/thor-go-sdk/thorgo"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

func B3trTransfer(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*EventIndexer, error) {
	b3tr := thor.Account(contracts.Vot3Address).Contract(contracts.Vot3ABI)
	criteria, err := b3tr.EventCriteria("Transfer")
	if err != nil {
		return nil, err
	}

	createTableSQL := `
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
	`

	processLogs := func(events []client.EventLog) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		decoded, err := b3tr.DecodeEvents(events)
		if err != nil {
			return err
		}

		stmt, err := tx.Prepare(`
			INSERT INTO b3tr_transfers ("from", "to", value, block_number, block_id, tx_id, clause_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, ev := range decoded {
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
		[]client.EventCriteria{criteria},
		1000,
		"b3tr_transfers",
		createTableSQL,
		processLogs,
	)
}

func B3trApproval(ctx context.Context, thor *thorgo.Thor, db *sql.DB) (*EventIndexer, error) {
	b3tr := thor.Account(contracts.B3trAddress).Contract(contracts.B3trABI)
	criteria, err := b3tr.EventCriteria("Approval")
	if err != nil {
		return nil, err
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS b3tr_approvals (
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

	processLogs := func(events []client.EventLog) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		decoded, err := b3tr.DecodeEvents(events)
		if err != nil {
			return err
		}

		stmt, err := tx.Prepare(`
			INSERT INTO b3tr_approvals ("owner", "spender", value, block_number, block_id, tx_id, clause_index)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, ev := range decoded {
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
		[]client.EventCriteria{criteria},
		1000,
		"b3tr_approvals",
		createTableSQL,
		processLogs,
	)
}
