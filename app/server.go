package main

import (
	"bufio"
	"context"
	"flag"
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

type serverConfig struct {
	port          int
	masterDetails string
	actAsReplica  bool
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

	fmt.Printf("server is listening on port as replica -> %d...", config.server.port)
	ctx, cancel := context.WithCancel(context.Background())
	cm := newConnectionManager()

	actAsReplica(config)
	if config.server.actAsReplica {
		go connectToMasterAsReplica(config, ctx, cm, store)
	}

	defer func() {
		listener.Close()
		cancel()
	}()

	for {
		conn, err := listener.Accept()
		c.activeClients.Add(1)
		fmt.Println("Global counter is ", c.activeClients.Load())
		if err != nil {
			fmt.Println("Error in lister.Accept() connection, ", err)
			continue
		}
		go handleConnection(conn, c, store, config, cm)

	}
}

func connectToMasterAsReplica(config *config, ctx context.Context, cm *connectionManager, store *redisStore) {

	masterHost, masterPort := func(args []string) (string, string) {
		return args[0], args[1]
	}(strings.Split(config.server.masterDetails, " "))

	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		fmt.Println("Error connecting to master as replica", err)
		return
	}

	// commands := []string{
	// 	"*1\r\n$4\r\nPING\r\n",
	// 	"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n",
	// 	"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
	// 	"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"}

	// for _, cmd := range commands {
	// 	if _, err := sendCommand(conn, cmd); err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}
	// }

	pingCommand := "*1\r\n$4\r\nPING\r\n"
	_, err = conn.Write([]byte(pingCommand))
	if err != nil {
		fmt.Println("Error sending PING", err)
		return
	}
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		if err == io.EOF {
			fmt.Println("Master closed the connection.")
		} else {
			fmt.Println("Error reading master response into replica", err)

		}
		return
	}

	fmt.Println("Buffer value is", string(buffer[:n]))

	replConfListeningCommand := "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
	_, err = conn.Write([]byte(replConfListeningCommand))
	if err != nil {
		fmt.Println("Error sending replConfListeningCommand", err)
		return
	}
	n, err = conn.Read(buffer)
	if err != nil {
		if err == io.EOF {
			fmt.Println("Master closed the connection.")
		} else {
			fmt.Println("Error reading master response into replica", err)

		}
		return
	}
	fmt.Println("Buffer value is", string(buffer[:n]))

	replConfSyncCommand := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	_, err = conn.Write([]byte(replConfSyncCommand))
	if err != nil {
		fmt.Println("Error sending replConfSyncCommand", err)
		return
	}
	n, err = conn.Read(buffer)
	if err != nil {
		if err == io.EOF {
			fmt.Println("Master closed the connection.")
		} else {
			fmt.Println("Error reading master response into replica", err)

		}
		return
	}
	fmt.Println("Buffer value is", string(buffer[:n]))

	pSyncCommand := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err = conn.Write([]byte(pSyncCommand))
	if err != nil {
		fmt.Println("Error sending pSyncCommand", err)
		return
	}

	reader := bufio.NewReader(conn)

	_, err = reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error while reading first line", err)
	}

	rdbSize, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading RDB size", err)
	}
	// fmt.Println("rdb size is", strings.TrimPrefix(rdbSize, "$"))
	rdbSize = strings.TrimSuffix(rdbSize, "\r\n")
	rdbSize = strings.TrimPrefix(rdbSize, "$")
	rdbByteCount, err := strconv.Atoi(rdbSize)
	if err != nil {
		fmt.Println("Error converting rdb file size byte values to int", err)
	}

	io.CopyN(io.Discard, reader, int64(rdbByteCount))

	for {

		command, args, err := parseRESPString(reader)
		fmt.Println("Command and args are", command, args)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Finished reading all commands and io.EOF reached")
			}
			fmt.Println("Error parsing parseRESPString", err)
			break
		}

		output, err := handleCommand(conn, command, args, store, config, cm)
		if err != nil {
			fmt.Println("error from redisInput parser", err)
		} else {
			conn.Write([]byte(output))
		}
	}

	<-ctx.Done()
	conn.Close()
}

func sendCommand(conn net.Conn, command string) (string, error) {
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("error sending command: %w", err)
	}

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("error reading response: %w", err)
	}
	fmt.Println("Buffer value is", string(buffer[:n]))

	return string(buffer[:n]), nil
}

func actAsReplica(c *config) {
	if c.server.masterDetails != "" {
		c.server.actAsReplica = true
	}
}

func parseFlags() *config {
	var config config
	flag.StringVar(&config.rdb.dir, "dir", "", "RDB directory path")
	flag.StringVar(&config.rdb.dbFileName, "dbfilename", "", "RDB file name")
	flag.IntVar(&config.server.port, "port", 6379, "Port number for redis server")
	flag.StringVar(&config.server.masterDetails, "replicaof", "", "Master details to run on a replica")

	flag.Parse()
	return &config
}

func handleConnection(conn net.Conn, c *clientData, store *redisStore, config *config, cm *connectionManager) {
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

		output, err := handleCommand(conn, command, args, store, config, cm)
		if err != nil {
			fmt.Println("error from redisInput parser", err)
		} else {
			conn.Write([]byte(output))
		}
	}
}
