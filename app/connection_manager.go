package main

import (
	"fmt"
	"net"
	"sync"
)

type connectionManager struct {
	mu       sync.Mutex
	replicas map[string]net.Conn
	clients  map[string]net.Conn
}

func newConnectionManager() *connectionManager {
	return &connectionManager{
		replicas: make(map[string]net.Conn),
		clients:  make(map[string]net.Conn),
	}
}

func (cm *connectionManager) addConnection(addr string, conn net.Conn, connType string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	switch connType {
	case "replica":
		cm.replicas[addr] = conn
	case "client":
		cm.clients[addr] = conn
	}
}

func (cm *connectionManager) removeConnection(addr string, connType string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	switch connType {
	case "replica":
		delete(cm.replicas, addr)
	case "client":
		delete(cm.clients, addr)
	}
}

func (cm *connectionManager) propagateCommandsToReplica(respArray string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for addr, conn := range cm.replicas {
		conn.Write([]byte(respArray))
		fmt.Println("Propagated command to: ", addr)
	}
}
