package entities

import (
	"encoding/json"
	"strconv"
)

type Account struct {
	AccountID int     `json:"account_id" valid:"required"`
	Balance   float64 `json:"balance" valid:"required"`
}

func (a *Account) UnmarshalJSON(data []byte) error {
	type Alias Account
	aux := &struct {
		*Alias
		Balance string `json:"balance"`
	}{
		Alias: (*Alias)(a),
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	balance, err := strconv.ParseFloat(aux.Balance, 64)
	if err != nil {
		return err
	}
	a.Balance = balance
	return nil
}
