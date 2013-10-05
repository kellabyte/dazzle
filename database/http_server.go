package database

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
	"time"
)

type HttpServer struct {
	address   string
	port      int
	listening bool
	router    *mux.Router
	db        *Database
}

func NewHttpServer(db *Database) (*HttpServer, error) {
	server := &HttpServer{
		db:     db,
		router: mux.NewRouter(),
	}
	return server, nil
}

func (httpServer *HttpServer) ListenAndServe(address string, port int) error {
	fmt.Printf("Listening on %s:%d\n", address, port)

	server := &http.Server{
		Addr:           address + ":" + strconv.Itoa(port),
		Handler:        httpServer.router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	httpServer.router.HandleFunc("/db", httpServer.getHandler).Methods("GET")
	httpServer.router.HandleFunc("/db", httpServer.setHandler).Methods("POST")
	httpServer.router.HandleFunc("/db", httpServer.setHandler).Methods("PUT")

	server.ListenAndServe()
	return nil
}

// This is a hack around Gorilla mux not providing the correct net/http HandleFunc() interface.
func (httpServer *HttpServer) HandleFunc(pattern string,
	handler func(http.ResponseWriter, *http.Request)) {

	httpServer.router.HandleFunc(pattern, handler)
}

func (server *HttpServer) getHandler(response http.ResponseWriter, request *http.Request) {
	//vars := mux.Vars(request)
	tx, err := server.db.BeginTransaction()

	val, err := tx.Get("1234/first_name")
	if err != nil {
		fmt.Println("Can't get key")
	} else {
		fmt.Fprintf(response, "value: "+val)
	}

	tx.Abort()
}

func (server *HttpServer) setHandler(response http.ResponseWriter, request *http.Request) {
	command := NewSetCommand()

	if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := server.db.raftPeer.ExecuteCommand(command); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
}
