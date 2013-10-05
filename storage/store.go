package storage

type Store interface {
	Open(path string) error
	Close() error
	BeginTransaction(readOnly bool) (*Transaction, error)
	CommitTransaction(tx *Transaction) error
	AbortTransaction(tx *Transaction) error
	Sync() error
	Get(tx interface{}, key string) (string, error)
	Set(tx interface{}, key string, value string) error
}
