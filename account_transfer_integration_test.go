package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"path/filepath"
	"sync"
	"takeHomeAssignment/db"
	"testing"
)

func CreatePostgresContainer(ctx context.Context) (*sql.DB, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:12",
		ExposedPorts: []string{"5432/tcp"},
		AutoRemove:   true,
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	// Create the tables in the database
	initSQLPath := filepath.Join(".", "db", "init.sql")
	initSQL, err := os.ReadFile(initSQLPath)
	if err != nil {
		return nil, err
	}

	// Get the mapped port for the container
	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return nil, err
	}

	// Create a database connection
	database, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable", mappedPort.Int()))
	if err != nil {
		return nil, err
	}

	// Execute the SQL script to create the tables
	_, err = database.Exec(string(initSQL))
	if err != nil {
		return nil, err
	}

	return database, nil

}

func TestQueryAccountByAccountId(t *testing.T) {
	database, err := CreatePostgresContainer(context.Background())
	defer database.Close()

	assert.NoError(t, err)
	account := db.Account{AccountID: 123, Balance: 123.0}
	err = db.CreateAccount(database, &account)
	assert.NoError(t, err)
	val := db.Account{}
	err = database.QueryRow("SELECT account_id, balance FROM account_balance WHERE account_id = $1", 123).Scan(&val.AccountID, &val.Balance)
	assert.NoError(t, err)
	assert.Equal(t, float64(123), account.Balance)
}

func TestProcessTransaction(t *testing.T) {
	tests := []struct {
		name          string
		transaction   db.Transaction
		sourceAccount db.Account
		destAccount   db.Account
		expectedError error
	}{
		{
			name:          "Valid transaction",
			sourceAccount: db.Account{AccountID: 1, Balance: 1000.0},
			destAccount:   db.Account{AccountID: 2, Balance: 1000.0},
			transaction: db.Transaction{
				SourceAccountID:      1,
				DestinationAccountID: 2,
				Amount:               "100.0",
			},
			expectedError: nil,
		},
		{
			name:          "Source account not found throws error",
			sourceAccount: db.Account{},
			destAccount:   db.Account{AccountID: 2, Balance: 1000.0},
			transaction: db.Transaction{
				SourceAccountID:      3,
				DestinationAccountID: 2,
				Amount:               "100.0",
			},
			expectedError: sql.ErrNoRows,
		},
		{
			name:          "Destination account not found throws error",
			sourceAccount: db.Account{AccountID: 2, Balance: 1000.0},
			destAccount:   db.Account{},
			transaction: db.Transaction{
				SourceAccountID:      1,
				DestinationAccountID: 4,
				Amount:               "100.0",
			},
			expectedError: sql.ErrNoRows,
		},
		{
			name:          "Source account has insufficient balance throws error",
			sourceAccount: db.Account{AccountID: 1, Balance: 1000.0},
			destAccount:   db.Account{AccountID: 2, Balance: 0.0},
			transaction: db.Transaction{
				SourceAccountID:      2,
				DestinationAccountID: 1,
				Amount:               "100.0",
			},
			expectedError: &pq.Error{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database, err := CreatePostgresContainer(context.Background())
			defer database.Close()

			assert.NoError(t, err)

			if tt.name != "Source account not found throws error" {
				err = db.CreateAccount(database, &tt.sourceAccount)
				assert.NoError(t, err)
			}
			if tt.name != "Destination account not found throws error" {
				err = db.CreateAccount(database, &tt.destAccount)
				assert.NoError(t, err)
			}

			// When transferring from one account to the other
			err = db.ProcessTransaction(database, &tt.transaction)
			// Expect error or no error
			if tt.expectedError != nil {
				if errors.Is(tt.expectedError, sql.ErrNoRows) {
					assert.Error(t, err)
					assert.Equal(t, tt.expectedError, err)
				}
				if errors.Is(tt.expectedError, &pq.Error{}) {
					var pqErr *pq.Error
					assert.True(t, errors.As(err, &pqErr))
					assert.Equal(t, "23514", pqErr.Code)
				}

			} else {
				assert.NoError(t, err)
			}

		})
	}
}

func TestConcurrentTransactions(t *testing.T) {
	// Create a new PostgreSQL container
	database, err := CreatePostgresContainer(context.Background())
	assert.NoError(t, err)
	defer database.Close()

	// Create 3 accounts with initial balances
	account1 := db.Account{AccountID: 1, Balance: 1000.0}
	account2 := db.Account{AccountID: 2, Balance: 1000.0}
	account3 := db.Account{AccountID: 3, Balance: 1000.0}

	err = db.CreateAccount(database, &account1)
	assert.NoError(t, err)
	err = db.CreateAccount(database, &account2)
	assert.NoError(t, err)
	err = db.CreateAccount(database, &account3)
	assert.NoError(t, err)

	// Define a function to process a transaction
	processTransaction := func(transaction *db.Transaction) {
		err = db.ProcessTransaction(database, transaction)
	}

	// Create a wait group to wait for all transactions to complete
	var wg sync.WaitGroup

	// Process 100 concurrent transactions from account 1 to account 2
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			transaction := db.Transaction{
				SourceAccountID:      1,
				DestinationAccountID: 2,
				Amount:               "1.0",
			}
			processTransaction(&transaction)
			wg.Done()
		}()
	}

	// Process 100 concurrent transactions from account 3 to account 2
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			transaction := db.Transaction{
				SourceAccountID:      3,
				DestinationAccountID: 2,
				Amount:               "1.0",
			}
			processTransaction(&transaction)
			wg.Done()
		}()
	}

	// Wait for all transactions to complete
	wg.Wait()

	// Check the final balances
	var balance1 float64
	var balance2 float64
	var balance3 float64
	err = database.QueryRow("SELECT balance FROM account_balance WHERE account_id = $1", 1).Scan(&balance1)
	assert.NoError(t, err)
	err = database.QueryRow("SELECT balance FROM account_balance WHERE account_id = $1", 2).Scan(&balance2)
	assert.NoError(t, err)
	err = database.QueryRow("SELECT balance FROM account_balance WHERE account_id = $1", 3).Scan(&balance3)
	assert.NoError(t, err)

	// Check that the balances sum up to 3000, some might fail but the total value should be kept constant
	assert.Equal(t, 3000.0, balance1+balance2+balance3)

	// Check that the total transactions sum up
	query := `SELECT 
               (COALESCE(SUM(CASE WHEN account_transfer_in = $1 THEN amount ELSE 0 END), 0) - 
                COALESCE(SUM(CASE WHEN account_transfer_out = $1 THEN amount ELSE 0 END), 0)) AS balance
               FROM 
               account_transactions
               WHERE 
               account_transfer_out = $1 OR account_transfer_in = $1`

	// Check that the account balance for account 1 sum up to transaction table
	row := database.QueryRow(query, 1)

	var transactionNetBalance float64
	err = row.Scan(&transactionNetBalance)
	assert.NoError(t, err)
	assert.Equal(t, balance1, 1000+transactionNetBalance)

	// Check that the account balance for account 2 sum up to transaction table
	row = database.QueryRow(query, 2)
	err = row.Scan(&transactionNetBalance)
	assert.NoError(t, err)
	assert.Equal(t, balance2, 1000+transactionNetBalance)

	// Check that the account balance for account 3 sum up to transaction table
	row = database.QueryRow(query, 3)
	err = row.Scan(&transactionNetBalance)
	assert.NoError(t, err)
	assert.Equal(t, balance3, 1000+transactionNetBalance)
}
