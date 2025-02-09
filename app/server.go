package main

import (
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

func main() {
	c := &clientData{}
	c.activeClients.Store(0)

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("server is listening on port 6379...")

	var wg sync.WaitGroup

	for {
		conn, err := listener.Accept()
		c.activeClients.Add(1)
		fmt.Println("Global counter is ", c.activeClients.Load())
		if err != nil {
			fmt.Println("Error in lister.Accept() connection, ", err)
			continue
		}
		wg.Add(1)
		go func() {
			handleConnection(conn, &wg, c)
		}()
	}
}

func handleConnection(conn net.Conn, wg *sync.WaitGroup, c *clientData) {
	defer wg.Done()
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected.")
			} else {
				fmt.Println("Error in conn.Read()", err)
			}
			c.activeClients.Add(^uint32(0))
			fmt.Println("Current active clients is ", c.activeClients.Load())
			return
		}
		bufferString := string(buf[:n])

		outputBytes, err := redisInputParser(bufferString)

		if err != nil {
			fmt.Println("error from redisInput parser", err)
		} else {
			conn.Write(outputBytes)
		}

	}
}

func redisInputParser(clientInput string) ([]byte, error) {
	var input []string
	output := make([]byte, 0, 256)

	input = strings.Split(clientInput, "\r\n")
	firstElement := input[0]

	if strings.HasPrefix(firstElement, "*") {
		crlfSeparatedValuesCount, _ := strconv.Atoi(firstElement[1:])
		forCount := crlfSeparatedValuesCount * 2
		currentCommand := ""

		for i := 1; i <= forCount; i++ {

			if strings.HasPrefix(input[i], "$") {
				numberWithoutDollar := strings.ReplaceAll(input[i], "$", "")
				_, err := strconv.Atoi(numberWithoutDollar)
				if err != nil {
					continue
				}
			} else {
				switch strings.ToLower(input[i]) {
				case "ping":
					output = append(output, []byte("+PONG\r\n")...)
					return []byte(output), nil
				case "echo":
					currentCommand = "echo"
				default:

					if currentCommand == "echo" {
						respString := fmt.Sprintf("$%d\r\n%s\r\n", len(input[i]), input[i])
						return []byte(respString), nil
					} else {
						return []byte{}, errors.New("invalid command")
					}

				}

			}

		}

	}
	return []byte(output), nil
}
