package database

import (
	"github.com/kellabyte/dazzle/storage"
	"log"
	"os"
	"path"
)

type Database struct {
	httpServer *HttpServer
	store      storage.Store
	raftPeer   *RaftPeer
}

func NewDatabase(name string, host string, port int, directoryPath string, leaderAddress string) (*Database, error) {
	db := &Database{}

	httpServer, err := NewHttpServer(db)
	if err != nil {
		return nil, err
	}

	dataPath := path.Join(directoryPath, "/data")
	if err := os.MkdirAll(dataPath, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}

	log.Println("Opening " + dataPath)

	store, err := storage.NewLMDBStore()
	store.Open(dataPath)
	db.httpServer = httpServer
	db.store = store

	raftPeer, err := NewRaftPeer(name, host, port, directoryPath, leaderAddress, db)
	db.raftPeer = raftPeer

	raftPeer.StartBatching()

	return db, nil
}

func (db *Database) ListenAndServe(address string, port int) error {
	err := db.raftPeer.ListenAndServe(db.httpServer.router, db.httpServer)
	err = db.httpServer.ListenAndServe(address, port)
	return err
}

func (db *Database) BeginTransaction() (*storage.Transaction, error) {
	tx, err := db.store.BeginTransaction(false)
	if err != nil {
		return nil, err
	}
	return tx, nil
}
