package storage

import ()

type Transaction struct {
	store   Store
	context interface{}
}

func NewTransaction(store Store) (*Transaction, error) {
	tx := &Transaction{}
	tx.store = store
	return tx, nil
}

func (tx *Transaction) Commit() error {
	tx.store.CommitTransaction(tx)
	return nil
}

func (tx *Transaction) Abort() error {
	tx.store.CommitTransaction(tx)
	return nil
}

func (tx *Transaction) Get(key string) (string, error) {
	val, err := tx.store.Get(tx.context, key)
	if err != nil {
		return "", err
	}
	return val, nil
}

func (tx *Transaction) Set(key string, value string) error {
	err := tx.store.Set(tx.context, key, value)
	if err != nil {
		return err
	}
	return nil
}
