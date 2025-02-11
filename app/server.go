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

type value struct {
	content string
	expiry  int64
}

type redisStore struct {
	mu    sync.RWMutex
	store map[string]value
}

func (r *redisStore) Set(args []string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	var expiry int64

	//[key, value, timeUnit, expiry] - for set with expiry
	//[key, value] - set without expiry
	if len(args) == 4 {
		if strings.ToLower(args[2]) == "px" {
			expiryInt, err := strconv.Atoi(args[3])
			if err != nil {
				return false
			} else {
				expiry = time.Now().Add(time.Millisecond * time.Duration(expiryInt)).UnixNano()
			}
		}
	} else {
		expiry = 0
	}

	r.store[args[0]] = value{
		content: args[1],
		expiry:  expiry,
	}
	return true
}

func (r *redisStore) Get(key string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if val, ok := r.store[key]; ok {
		if val.expiry == 0 {
			return val.content, nil
		} else {
			if expired(val.expiry) {
				return "", errors.New("err - value expired")
			} else {
				return val.content, nil
			}
		}

	}
	return "", errors.New("err - no value for this key")
}

func expired(expiryTime int64) bool {
	return time.Now().UnixNano() >= expiryTime
}

func main() {

	c := &clientData{}
	c.activeClients.Store(0)
	store := &redisStore{store: map[string]value{}}

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
		if len(args) < 2 {
			return "", errors.New("ERR wrong number of arguments for 'set' command")
		}
		if store.Set(args) {
			return "+OK\r\n", nil
		}
		return "", errors.New("err - setting value")
	case "get":
		string, err := store.Get(args[0])
		if err != nil {
			return "$-1\r\n", nil
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(string), string), nil

	default:
		return "", errors.New("err - unknown command")
	}
}
