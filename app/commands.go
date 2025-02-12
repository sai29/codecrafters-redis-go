package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

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

func (c *config) getRDBConfig(args []string) (string, error) {
	var output string
	if args[0] == "get" {
		if args[1] == "dir" {
			output = c.rdb.dir
		} else if args[1] == "rdbfilename" {
			output = c.rdb.dbFileName
		}
		return respArrayGenerator(args[1], output), nil
	}
	return "", errors.New("err - unknown argument")
}

func handleCommand(command string, args []string, store *redisStore, config *config) (string, error) {
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
	case "config":
		return config.getRDBConfig(args)
	default:
		return "", errors.New("err - unknown command")
	}
}
