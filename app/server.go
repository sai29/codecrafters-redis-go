package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type clientData struct {
	activeClients atomic.Uint32
}

type redisStore struct {
	mu    sync.RWMutex
	store map[string]string
}

func (r *redisStore) Set(key, val string) {
	r.mu.Lock()
	r.store[key] = val
	r.mu.Unlock()
}

func (r *redisStore) Get(key string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if val, ok := r.store[key]; ok {
		return val, nil
	}
	return "", errors.New("err - no value for this key")
}
func main() {
	c := &clientData{}
	c.activeClients.Store(0)
	store := &redisStore{store: map[string]string{}}

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("server is listening on port 6379...")

	for {
		conn, err := listener.Accept()
		c.activeClients.Add(1)
		// fmt.Println("Global counter is ", c.activeClients.Load())
		if err != nil {
			fmt.Println("Error in lister.Accept() connection, ", err)
			continue
		}
		go handleConnection(conn, c, store)

	}
}

func handleConnection(conn net.Conn, c *clientData, store *redisStore) {
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	reader := bufio.NewReader(conn)

	for {

		command, args, err := parseRESPString(reader)
		if err != nil {
			fmt.Println("Error parsing RESP string", err)
		}

		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected.")
				c.activeClients.Add(^uint32(0))
				fmt.Println("Current active clients is ", c.activeClients.Load())
				break
			} else {
				continue
			}

		}

		output, err := handleCommand(command, args, store)

		if err != nil {
			fmt.Println("error from redisInput parser", err)
		} else {
			conn.Write([]byte(output))
		}

	}
}

func parseRESPString(reader *bufio.Reader) (string, []string, error) {

	header, _, err := reader.ReadLine()
	if err != nil {
		return "", nil, err
	}

	if len(header) == 0 || header[0] != '*' {
		return "", nil, fmt.Errorf("invalid RESP header")
	}

	argSize, err := parseRESPInteger(string(header[1:]), 1, "invalid array size: %q (must be >= 1)")
	if err != nil {
		return "", nil, err
	}

	var command string
	var args []string

	for i := 0; i < argSize; i++ {
		line, _, err := reader.ReadLine()
		if err != nil {
			return "", nil, err
		}

		if len(line) == 0 || line[0] != '$' {
			return "", nil, fmt.Errorf("invalid bulk string header")
		}

		strLength, err := parseRESPInteger(string(line[1:]), 0, "invalid string length: %q (must be >= 0)")
		if err != nil {
			return "", nil, err
		}

		stringBytes := make([]byte, strLength)
		_, err = io.ReadFull(reader, stringBytes)
		if err != nil {
			return "", nil, err
		}

		reader.Discard(2)

		if i == 0 {
			command = strings.ToLower(string(stringBytes))
		} else {
			args = append(args, string(stringBytes))
		}
	}

	return command, args, nil

}

func parseRESPInteger(s string, min int, errorFormat string) (int, error) {
	val, err := strconv.Atoi(s)
	if err != nil || val < min {
		return 0, fmt.Errorf(errorFormat, s)
	}
	return val, nil
}

func handleCommand(command string, args []string, store *redisStore) (string, error) {
	switch command {
	case "ping":
		return "+PONG\r\n", nil
	case "echo":
		if len(args) == 0 {
			return "", errors.New("err - wrong number of arguments")
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0]), nil
	case "set":
		store.Set(args[0], args[1])
		return "+OK\r\n", nil
	case "get":
		string, err := store.Get(args[0])
		if err != nil {
			return fmt.Sprintf("$%d\r\n", -1), nil
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(string), string), nil

	default:
		return "", errors.New("err - unknown command")
	}
}
