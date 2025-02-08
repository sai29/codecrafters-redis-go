package main

import (
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

func main() {
	c := &clientData{}
	c.activeClients.Store(0)

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Server is listening on port 6379...")

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
		_, err := conn.Read(buf)
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
		conn.Write([]byte("+PONG\r\n"))
	}
}
