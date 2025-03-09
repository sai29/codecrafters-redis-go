package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type clientData struct {
	activeClients atomic.Uint32
}

type serverConfig struct {
	port int
}

type rdbConfig struct {
	dir        string
	dbFileName string
}

type value struct {
	content string
	expiry  int64
}

type redisStore struct {
	mu    sync.RWMutex
	store map[string]value
}

type config struct {
	rdb    rdbConfig
	server serverConfig
}

func main() {
	c := &clientData{}
	c.activeClients.Store(0)
	store := &redisStore{store: map[string]value{}}

	config := parseFlags()

	port := fmt.Sprintf("0.0.0.0:%d", config.server.port)

	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error starting server:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Printf("server is listening on port -> %d...", config.server.port)

	for {
		conn, err := listener.Accept()
		c.activeClients.Add(1)
		fmt.Println("Global counter is ", c.activeClients.Load())
		if err != nil {
			fmt.Println("Error in lister.Accept() connection, ", err)
			continue
		}
		go handleConnection(conn, c, store, config)

	}
}

func parseFlags() *config {
	var config config
	flag.StringVar(&config.rdb.dir, "dir", "", "RDB directory path")
	flag.StringVar(&config.rdb.dbFileName, "dbfilename", "", "RDB file name")
	flag.IntVar(&config.server.port, "port", 6379, "Port number for redis server")

	flag.Parse()
	return &config
}

func handleConnection(conn net.Conn, c *clientData, store *redisStore, config *config) {
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	reader := bufio.NewReader(conn)

	for {

		command, args, err := parseRESPString(reader)
		if err != nil {
			fmt.Println("Error parsing RESP string", err)
			if err == io.EOF {
				fmt.Println("Client disconnected.")
				c.activeClients.Add(^uint32(0))
				fmt.Println("Current active clients is ", c.activeClients.Load())
				break
			} else {
				continue
			}
		}

		output, err := handleCommand(command, args, store, config)
		if err != nil {
			fmt.Println("error from redisInput parser", err)
		} else {
			conn.Write([]byte(output))
		}

	}
}
