package entities

import (
	"encoding/json"
	"errors"
	"strconv"
)

type Transaction struct {
	SourceAccountID      int     `json:"source_account_id" valid:"required"`
	DestinationAccountID int     `json:"destination_account_id" valid:"required"`
	Amount               float64 `json:"amount" valid:"required"`
}

func (t *Transaction) UnmarshalJSON(data []byte) error {
	type Alias Transaction
	aux := &struct {
		*Alias
		Amount string `json:"amount"`
	}{
		Alias: (*Alias)(t),
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	amount, err := strconv.ParseFloat(aux.Amount, 64)
	if err != nil {
		return err
	}
	t.Amount = amount

	// Check for extra fields
	var temp map[string]interface{}
	err = json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}
	for key := range temp {
		if key != "source_account_id" && key != "destination_account_id" && key != "amount" {
			return errors.New("extra field found")
		}
	}
	return nil
}
