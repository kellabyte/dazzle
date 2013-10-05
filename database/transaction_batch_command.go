package database

import (
	"fmt"
	"github.com/goraft/raft"
)

// A Transaction is simply a collection of commands batched together.
type TransactionBatchCommand struct {
	Commands []raft.Command
}

func (t *TransactionBatchCommand) CommandName() string {
	return "txn"
}

func (t *TransactionBatchCommand) Apply(s *raft.Server) (interface{}, error) {
	fmt.Println(len(t.Commands))
	for _, c := range t.Commands {
		fmt.Println("COMMANDS!!!")
		if _, err := c.Apply(s); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
