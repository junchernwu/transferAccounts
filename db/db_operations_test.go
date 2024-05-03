package db

import (
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestQueryAccountByAccountId(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"account_id", "balance"}).
		AddRow(1, 100.0).
		AddRow(2, 200.0)

	mock.ExpectQuery(`SELECT account_id, balance FROM account_balance WHERE account_id = \$1`).WithArgs(1).WillReturnRows(rows)

	account := Account{}
	err = QueryAccountByAccountId(db, 1, &account)
	assert.Nil(t, err)
	assert.Equal(t, 1, account.AccountID)
	assert.Equal(t, 100.0, account.Balance)
}

func TestCreateAccount(t *testing.T) {
	tests := []struct {
		name                 string
		accountID            int
		balance              float64
		accountAlreadyExists bool
	}{
		{"Successfully creates account when account does not exist", 1, 100.0, false},
		{"Fails to create account when account already exists", 1, 100.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			account := Account{AccountID: tt.accountID, Balance: tt.balance}
			defer db.Close()
			if tt.accountAlreadyExists {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)
				mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM account_balance WHERE account_id = \$1\)`).WithArgs(account.AccountID).WillReturnRows(rows)
				err = CreateAccount(db, &account)
				assert.NotNil(t, err)
				assert.Equal(t, "account ID already exists", err.Error())
			} else {
				account := Account{AccountID: 1, Balance: 100.0}
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(false)
				mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM account_balance WHERE account_id = \$1\)`).WithArgs(account.AccountID).WillReturnRows(rows)

				mock.ExpectExec("INSERT INTO account_balance \\(account_id, balance\\) VALUES \\(\\$1, \\$2\\) RETURNING \\*").WithArgs(account.AccountID, account.Balance).WillReturnResult(sqlmock.NewResult(1, 1))
				err = CreateAccount(db, &account)
				assert.Nil(t, err)
			}

		})
	}
}

func TestProcessTransaction(t *testing.T) {
	tests := []struct {
		name          string
		transaction   Transaction
		expectedError error
	}{
		{
			name: "Valid transaction",
			transaction: Transaction{
				SourceAccountID:      1,
				DestinationAccountID: 2,
				Amount:               "100.0",
			},
			expectedError: nil,
		},
		{
			name: "Source account not found throws error",
			transaction: Transaction{
				SourceAccountID:      3,
				DestinationAccountID: 2,
				Amount:               "100.0",
			},
			expectedError: sql.ErrNoRows,
		},
		{
			name: "Destination account not found throws error",
			transaction: Transaction{
				SourceAccountID:      1,
				DestinationAccountID: 4,
				Amount:               "100.0",
			},
			expectedError: sql.ErrNoRows,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			defer db.Close()

			// Mock the database queries
			mock.ExpectBegin()
			if tt.name == "Source account not found throws error" {
				mock.ExpectQuery(`SELECT balance, updated_at FROM account_balance WHERE account_id = \$1 FOR UPDATE`).WithArgs(tt.transaction.SourceAccountID).WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectQuery(`SELECT balance, updated_at FROM account_balance WHERE account_id = \$1 FOR UPDATE`).WithArgs(tt.transaction.SourceAccountID).WillReturnRows(sqlmock.NewRows([]string{"balance", "updated_at"}).AddRow(100.0, time.Now()))
			}

			if tt.name == "Destination account not found throws error" {
				mock.ExpectQuery(`SELECT balance, updated_at FROM account_balance WHERE account_id = \$1 FOR UPDATE`).WithArgs(tt.transaction.DestinationAccountID).WillReturnError(sql.ErrNoRows)
			} else {
				mock.ExpectQuery(`SELECT balance, updated_at FROM account_balance WHERE account_id = \$1 FOR UPDATE`).WithArgs(tt.transaction.DestinationAccountID).WillReturnRows(sqlmock.NewRows([]string{"balance", "updated_at"}).AddRow(200.0, time.Now()))
			}

			if tt.expectedError == nil {
				mock.ExpectExec(`UPDATE account_balance SET balance = \$1, updated_at = \$2 WHERE account_id = \$3 AND updated_at = \$4`).WithArgs(0.0, sqlmock.AnyArg(), tt.transaction.SourceAccountID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(`UPDATE account_balance SET balance = \$1, updated_at = \$2 WHERE account_id = \$3 AND updated_at = \$4`).WithArgs(300.0, sqlmock.AnyArg(), tt.transaction.DestinationAccountID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(1, 1))

				mock.ExpectExec(`INSERT INTO account_transactions \(account_transfer_out, account_transfer_in, amount\) VALUES \(\$1, \$2, \$3\)`).WithArgs(tt.transaction.SourceAccountID, tt.transaction.DestinationAccountID, tt.transaction.Amount).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}

			// Call the ProcessTransaction function
			err = ProcessTransaction(db, &tt.transaction)

			assert.Equal(t, err, tt.expectedError)
		})
	}
}
