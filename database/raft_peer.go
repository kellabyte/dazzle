package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type RaftPeer struct {
	name          string
	host          string
	port          int
	path          string
	leaderAddress string
	raftServer    *raft.Server
	db            *Database
	batchChannel  chan raft.Command
}

type call struct {
	Cmd raft.Command
	Err chan error
}

func NewRaftPeer(name string, host string, port int, path string, leaderAddress string, db *Database) (*RaftPeer, error) {
	peer := &RaftPeer{
		name:          name,
		host:          host,
		port:          port,
		path:          path,
		leaderAddress: leaderAddress,
		db:            db,
	}
	return peer, nil
}

func SetLogging(verbose bool, trace bool, debug bool, host string,
	port int) {

	if verbose {
		log.Print("Verbose logging enabled.")
	}
	if trace {
		raft.SetLogLevel(raft.Trace)
		log.Print("Raft trace debugging enabled.")
	} else if debug {
		raft.SetLogLevel(raft.Debug)
		log.Print("Raft debugging enabled.")
	}
}

// Returns the connection string.
func (raftPeer *RaftPeer) connectionString() string {
	return fmt.Sprintf("http://%s:%d", raftPeer.host, raftPeer.port)
}

func (raftPeer *RaftPeer) ListenAndServe(router *mux.Router, httpServer raft.HTTPMuxer) error {
	var err error
	rand.Seed(time.Now().UnixNano())

	// Setup commands.
	raft.RegisterCommand(&TransactionBatchCommand{})
	raft.RegisterCommand(&SetCommand{})

	if err := os.MkdirAll(raftPeer.path, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}

	log.Printf("Initializing Raft Server: %s", raftPeer.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	//NewServer(name string, path string, transporter Transporter, stateMachine StateMachine, context interface{}, connectionString string) (*Server, error) {
	raftPeer.raftServer, err = raft.NewServer(raftPeer.name, raftPeer.path, transporter, nil, raftPeer.db, "")
	if err != nil {
		log.Fatal(err)
	}
	transporter.Install(raftPeer.raftServer, httpServer)
	raftPeer.raftServer.Start()

	// Join to leader if specified.
	if raftPeer.leaderAddress != "" {
		log.Println("Attempting to join leader:", raftPeer.leaderAddress)

		if !raftPeer.raftServer.IsLogEmpty() {
			log.Fatal("Cannot join with an existing log")
		}
		if err := raftPeer.Join(raftPeer.leaderAddress); err != nil {
			log.Fatal(err)
		}

		// Initialize the server by joining itself.
	} else if raftPeer.raftServer.IsLogEmpty() {
		log.Println("Initializing new cluster")

		_, err := raftPeer.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             raftPeer.raftServer.Name(),
			ConnectionString: raftPeer.connectionString(),
		})
		if err != nil {
			log.Fatal(err)
		}

	} else {
		log.Println("Recovered from log")
	}

	router.HandleFunc("/join", raftPeer.joinHandler).Methods("POST")

	fmt.Printf("Raft listening\n")
	return nil // TODO return errors.
}

func (raftPeer *RaftPeer) joinHandler(response http.ResponseWriter, request *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := raftPeer.raftServer.Do(command); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Joins to the leader of an existing cluster.
func (raftPeer *RaftPeer) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             raftPeer.raftServer.Name(),
		ConnectionString: raftPeer.connectionString(),
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	resp.Body.Close()
	if err != nil {
		return err
	}

	return nil
}

func (raftPeer *RaftPeer) StartBatching() {
	raftPeer.batchChannel = make(chan raft.Command, 1024)
	c := raftPeer.batchChannel

	go func() {
		for {
			txn := &TransactionBatchCommand{Commands: []raft.Command{}}

			// this avoids busy loop
			txn.Commands = append(txn.Commands, <-c)
		fill:
			for {
				select {
				case command := <-c:
					txn.Commands = append(txn.Commands, command)
					if len(txn.Commands) > 100 {
						break fill
					}

				default:
					break fill
				}
			}

			_, err := raftPeer.raftServer.Do(txn)

			for _, command := range txn.Commands {
				if command, ok := command.(*SetCommand); ok {
					command.C <- err
				}
			}
		}
	}()
}

func (raftPeer *RaftPeer) ExecuteCommand(command raft.Command) error {
	// Execute the command against the Raft server.
	//_, err := raftPeer.raftServer.Do(command)
	//return err

	if command, ok := command.(*SetCommand); ok {
		// Send command to be batched.
		raftPeer.batchChannel <- command

		// Receive the error, if any, for the transaction.
		err := <-command.C
		if err != nil {
			return err
		}
	}

	return nil
}
