package storage

import (
	"fmt"
	"github.com/szferi/gomdb"
)

type LMDBStore struct {
	env *mdb.Env
	dbi mdb.DBI
}

func NewLMDBStore() (*LMDBStore, error) {
	store := &LMDBStore{}
	return store, nil
}

func (store *LMDBStore) Open(path string) error {
	env, err := mdb.NewEnv()
	if err != nil {
		fmt.Println("Error opening LMDB environment")
	}

	err = env.SetMapSize(1048576000)
	err = env.Open(path, 0, 0664)
	//err = env.Open(path, mdb.NOSYNC, 0664)
	if err != nil {
		fmt.Println("Error opening database")
	}

	var txn *mdb.Txn
	txn, err = env.BeginTxn(nil, 0)
	if err != nil {
		fmt.Println("Cannot begin transaction: %s", err)
	}

	var dbi mdb.DBI
	dbi, err = txn.DBIOpen(nil, 0)
	txn.Commit()

	store.env = env
	store.dbi = dbi

	return nil
}

func (store *LMDBStore) Close() error {
	store.env.Close()
	return nil
}

func (store *LMDBStore) BeginTransaction(readOnly bool) (*Transaction, error) {
	tx, err := NewTransaction(store)

	var txn *mdb.Txn
	txn, err = store.env.BeginTxn(nil, 0)
	if err != nil {
		fmt.Println("Cannot begin transaction: %s", err)
	}

	tx.context = txn
	return tx, nil
}

func (store *LMDBStore) CommitTransaction(tx *Transaction) error {
	txn, ok := tx.context.(*mdb.Txn)
	if !ok {
		return nil
	}
	txn.Commit()
	return nil
}

func (store *LMDBStore) AbortTransaction(tx *Transaction) error {
	txn, ok := tx.context.(*mdb.Txn)
	if !ok {
		return nil
	}
	txn.Abort()
	return nil
}

func (store *LMDBStore) Sync() error {
	return nil
}

func (store *LMDBStore) Get(tx interface{}, key string) (string, error) {
	txn, ok := tx.(*mdb.Txn)
	if !ok {
		return "", nil
	}

	val, err := txn.Get(store.dbi, []byte(key))
	if err != nil {
		return "", err
	}
	return string(val), nil
}

func (store *LMDBStore) Set(tx interface{}, key string, value string) error {
	txn, ok := tx.(*mdb.Txn)
	if !ok {
		return nil
	}

	err := txn.Put(store.dbi, []byte(key), []byte(value), 0)
	if err != nil {
		return err
	}
	return nil
}
