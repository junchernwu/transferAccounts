package entities

import (
	"encoding/json"
	"errors"
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

	// Check for extra fields
	var temp map[string]interface{}
	err = json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}
	for key := range temp {
		if key != "account_id" && key != "balance" {
			return errors.New("extra field found")
		}
	}

	return nil
}
