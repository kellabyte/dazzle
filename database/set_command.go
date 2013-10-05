package database

import (
	"fmt"
	"github.com/goraft/raft"
)

/*
{
    "type": "tx",
    "ops": [
        {
            "op": "set",
            "ks": "users",
            "k": "1234",
            "v": "asmith"
        },
        {
            "op": "set",
            "ks": "users",
            "k": "1234/first_name",
            "v": "Anna"
        },
        {
            "op": "set",
            "ks": "user_groups",
            "k": "asmith",
            "v": "admin"
        }
    ]
}
*/

type Operation struct {
	Op string
	Ks string
	K  string
	V  string
}

type SetCommand struct {
	C    chan error
	Type string
	Ops  []Operation
}

func NewSetCommand() *SetCommand {
	return &SetCommand{C: make(chan error)}
}

// The name of the command in the log.
func (c *SetCommand) CommandName() string {
	return "set"
}

// Sets a value to a key.
func (command *SetCommand) Apply(server *raft.Server) (interface{}, error) {
	/*
		//log.Println("Apply()")
		db := server.Context().(*Database)

		tx, err := db.BeginTransaction()
		if err != nil {
			//log.Println("Apply() ERROR")
			tx.Abort()
			return nil, err
		}

		for _, op := range command.Ops {
			err = tx.Set(op.K, op.V)
			if err != nil {
				//log.Println("Apply() SET ERROR")
				tx.Abort()
				return nil, err
			}
			//log.Printf("Apply() SET K: %s V: %s", op.K, op.V)
		}
		tx.Commit()
		//tx.Abort()
		//log.Println("Apply() COMMIT")
	*/
	fmt.Println("APPLY")
	return nil, nil
}
