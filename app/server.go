package main

import (
	"fmt"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()
	fmt.Println("Server is listening on port 6379...")

	conn, err := listener.Accept()

	for {
		if err != nil {
			continue
		}
		fmt.Println("New client connected")

		handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	response := "+PONG\r\n"
	_, err := conn.Write([]byte(response))
	if err != nil {
		fmt.Println("Error sending response:", err)
	}
}
