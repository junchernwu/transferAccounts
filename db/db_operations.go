package db

import (
	"database/sql"
	"errors"
	"log"
	. "takeHomeAssignment/entities"
	"time"
)

func QueryAccountByAccountId(DB *sql.DB, accountID int, account *Account) error {
	err := DB.QueryRow("SELECT account_id, balance FROM account_balance WHERE account_id = $1", accountID).Scan(&account.AccountID, &account.Balance)
	if err != nil {
		return err
	}
	return nil
}

func CreateAccount(DB *sql.DB, account *Account) error {
	var exists bool
	// Check if account_id exists first before inserting
	err := DB.QueryRow("SELECT EXISTS (SELECT 1 FROM account_balance WHERE account_id = $1)", account.AccountID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("account ID already exists")
	}
	_, err = DB.Exec("INSERT INTO account_balance (account_id, balance) VALUES ($1, $2) RETURNING *", account.AccountID, account.Balance)
	if err != nil {
		return err
	}
	return nil
}

func ProcessTransaction(DB *sql.DB, transaction *Transaction) error {
	// Start a new transaction
	dbtx, err := DB.Begin()
	if err != nil {
		return err
	}
	defer func(tx *sql.Tx) {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Println("Failed to roll back:", err)
		}
	}(dbtx)

	// Get the current balance and updated_at time for the source account
	var sourceBalance float64
	var sourceUpdatedAt time.Time

	// Get the current balance and updated_at time for the destination account
	var destBalance float64
	var destUpdatedAt time.Time

	var sourceID int
	var destID int
	var transactionAmount float64

	// Because we are locking the smaller account ID first
	// The source and destination will be flipped if original sourceAccountId of transaction is greater
	// In this case we have to negate the transaction amount
	if transaction.SourceAccountID > transaction.DestinationAccountID {
		sourceID = transaction.DestinationAccountID
		destID = transaction.SourceAccountID
		transactionAmount = -1 * transaction.Amount // subtract from source
	} else {
		sourceID = transaction.SourceAccountID
		destID = transaction.DestinationAccountID
		transactionAmount = transaction.Amount
	}

	// Lock row of smaller ID(sourceID) then destID
	err = dbtx.QueryRow(`
    WITH source AS (
        SELECT balance, updated_at
        FROM account_balance
        WHERE account_id = $1::integer
        FOR UPDATE
    ), dest AS (
        SELECT balance, updated_at
        FROM account_balance
        WHERE account_id = $2::integer
        FOR UPDATE
    )
    SELECT source.balance, source.updated_at, dest.balance, dest.updated_at
    FROM source, dest
`, sourceID, destID).Scan(&sourceBalance, &sourceUpdatedAt, &destBalance, &destUpdatedAt)

	if err != nil {
		return err
	}

	// Calculate the new balances
	newSourceBalance := sourceBalance - transactionAmount
	newDestBalance := destBalance + transactionAmount

	// Update the account balances
	_, err = dbtx.Exec("UPDATE account_balance SET balance = $1, updated_at = $2 WHERE account_id = $3 AND updated_at = $4", newSourceBalance, time.Now(), sourceID, sourceUpdatedAt)
	if err != nil {
		return err
	}
	_, err = dbtx.Exec("UPDATE account_balance SET balance = $1, updated_at = $2 WHERE account_id = $3 AND updated_at = $4", newDestBalance, time.Now(), destID, destUpdatedAt)
	if err != nil {
		return err
	}

	// Insert the transaction
	_, err = dbtx.Exec("INSERT INTO account_transactions (account_transfer_out, account_transfer_in, amount) VALUES ($1, $2, $3)", transaction.SourceAccountID, transaction.DestinationAccountID, transaction.Amount)
	if err != nil {
		return err
	}

	// Commit the transaction
	err = dbtx.Commit()
	if err != nil {
		return err
	}

	return nil
}
