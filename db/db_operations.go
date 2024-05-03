package db

import (
	"database/sql"
	"errors"
	"log"
	"strconv"
	"time"
)

type Account struct {
	AccountID int     `json:"account_id" valid:"required"`
	Balance   float64 `json:"balance" valid:"required"`
}

type Transaction struct {
	SourceAccountID      int    `json:"source_account_id" valid:"required"`
	DestinationAccountID int    `json:"destination_account_id" valid:"required"`
	Amount               string `json:"amount" valid:"required"`
}

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

	// Lock both rows at the same time to prevent deadlock
	err = dbtx.QueryRow(`
    WITH source AS (
        SELECT balance, updated_at
        FROM account_balance
        WHERE account_id = $1
        FOR UPDATE
    ), dest AS (
        SELECT balance, updated_at
        FROM account_balance
        WHERE account_id = $2
        FOR UPDATE
    )
    SELECT source.balance, source.updated_at, dest.balance, dest.updated_at
    FROM source, dest
`, transaction.SourceAccountID, transaction.DestinationAccountID).Scan(&sourceBalance, &sourceUpdatedAt, &destBalance, &destUpdatedAt)

	if err != nil {
		return err
	}

	transactionAmount, err := strconv.ParseFloat(transaction.Amount, 64)
	if err != nil {
		return err
	}
	// Calculate the new balances
	newSourceBalance := sourceBalance - transactionAmount
	newDestBalance := destBalance + transactionAmount

	// Update the account balances
	_, err = dbtx.Exec("UPDATE account_balance SET balance = $1, updated_at = $2 WHERE account_id = $3 AND updated_at = $4", newSourceBalance, time.Now(), transaction.SourceAccountID, sourceUpdatedAt)
	if err != nil {
		return err
	}
	_, err = dbtx.Exec("UPDATE account_balance SET balance = $1, updated_at = $2 WHERE account_id = $3 AND updated_at = $4", newDestBalance, time.Now(), transaction.DestinationAccountID, destUpdatedAt)
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
