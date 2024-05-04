package entities

import (
	"encoding/json"
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
	return nil
}
