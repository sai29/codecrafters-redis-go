package main

import (
	"fmt"
	"net"
	"os"
)

func main() {

	listener, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer listener.Close()

	fmt.Println("Server is listening on port 8080")

	for {

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error: ", err)
			continue
		}

		conn.Write([]byte("+PONG\r\n"))
		conn.Close()
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	fmt.Println("Received data", buf[:n])

	conn.Write([]byte("+PONG\r\n"))
}
