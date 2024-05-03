-- Create the account_balance table
CREATE TABLE account_balance (
                                 id SERIAL PRIMARY KEY,
                                 account_id INTEGER NOT NULL,
                                 balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00 CHECK (balance >= 0),
                                 updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                 UNIQUE (account_id)
);

-- Create the transaction table
CREATE TABLE account_transactions (
                             id SERIAL PRIMARY KEY,
                             account_transfer_out INTEGER NOT NULL,
                             account_transfer_in INTEGER NOT NULL,
                             amount DECIMAL(15, 2) NOT NULL,
                             created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                             FOREIGN KEY (account_transfer_out) REFERENCES account_balance(account_id),
                             FOREIGN KEY (account_transfer_in) REFERENCES account_balance(account_id));

CREATE INDEX idx_transaction_account_transfer ON account_transactions (account_transfer_out, account_transfer_in);
