package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asaskevich/govalidator"
	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"log"
	"net/http"
	"strconv"
	. "takeHomeAssignment/db"
)

var DB *sql.DB

func main() {
	var err error
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"0.0.0.0", 5432, "myuser", "mypassword", "mydb",
	)
	DB, err = sql.Open("postgres", connString)
	if err != nil {
		log.Fatal(err)
	}
	defer func(DB *sql.DB) {
		err := DB.Close()
		if err != nil {
			log.Println(err)
		}
	}(DB)
	router := mux.NewRouter()
	router.HandleFunc("/accounts/{account_id}", getAccount).Methods("GET")
	router.HandleFunc("/accounts", createAccount).Methods("POST")
	router.HandleFunc("/transactions", addTransaction).Methods("POST")

	fmt.Println("Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", router))

}
func init() {
	govalidator.SetFieldsRequiredByDefault(true)
}

func createAccount(w http.ResponseWriter, r *http.Request) {
	var account Account
	err := json.NewDecoder(r.Body).Decode(&account)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Validate the account struct
	_, err = govalidator.ValidateStruct(account)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = CreateAccount(DB, &account)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(account)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func getAccount(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	account := Account{}
	accountID, err := strconv.Atoi(vars["account_id"])

	if err != nil {
		http.Error(w, "Invalid account ID. It must be an integer.", http.StatusBadRequest)
		return
	}

	err = QueryAccountByAccountId(DB, accountID, &account)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Account does not exist", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	err = json.NewEncoder(w).Encode(account)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func addTransaction(w http.ResponseWriter, r *http.Request) {
	var tx Transaction

	err := json.NewDecoder(r.Body).Decode(&tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Validate the account struct
	_, err = govalidator.ValidateStruct(tx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	transactionAmount, err := strconv.ParseFloat(tx.Amount, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if transactionAmount <= 0.0 {
		http.Error(w, "transaction amount cannot be less than or equals to zero", http.StatusBadRequest)
		return
	}

	// Check if both source and destination are the same, no updates needed
	if tx.DestinationAccountID == tx.SourceAccountID {
		http.Error(w, "Transferring to the same account is not allowed", http.StatusBadRequest)
		return
	}

	// Check that both accounts exist
	account := Account{}

	err = QueryAccountByAccountId(DB, tx.DestinationAccountID, &account)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Println("Destination Account does not exist during transfer of funds:", err)
			http.Error(w, "Destination Account does not exist", http.StatusNotFound)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = QueryAccountByAccountId(DB, tx.SourceAccountID, &account)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Source Account does not exist", http.StatusNotFound)
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Check that the transfer out account has sufficient balance
	var transfer_amount float64
	transfer_amount, err = strconv.ParseFloat(tx.Amount, 64)
	if account.Balance <= transfer_amount {
		http.Error(w, "Insufficient balance for transaction to happen", http.StatusBadRequest)
		return
	}
	// Perform the transfer
	// Retry the transaction up to 3 times if there is a concurrency error
	for i := 0; i < 3; i++ {
		err = ProcessTransaction(DB, &tx)
		if err == nil {
			break
		}
		if err != nil {
			var pqErr *pq.Error
			if errors.As(err, &pqErr) {
				// 23514 is the PostgreSQL error code for check constraint violation
				if pqErr.Code == "23514" {
					http.Error(w, "Insufficient balance for transaction to happen", http.StatusBadRequest)
					// No need for retry because it is not a concurrency issue
					return
				} else {
					log.Println("Unknown error when processing transaction:", err)
				}
			} else {
				log.Println("Unknown error when processing transaction:", err)
			}

		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	response := "Transaction successful"
	_, err = w.Write([]byte(response))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
