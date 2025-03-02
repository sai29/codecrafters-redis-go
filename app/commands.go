package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// redis-cli KEYS "*" - Get all redis keys from rdb file - only single key
// redis-cli GET "foo" - Get a single value from the RDB file - string value
// redis-cli KEYS "*" - Get all redis keys from rdb file - Multiple keys
// redis-cli GET "foo",  redis-cli GET "bar" - Multiple keys at the same time hitting rdb file
// redis-cli GET "foo",  redis-cli GET "bar" - Multiple keys at the same time hitting rdb file with expiry for some.

type rdbStore struct {
	store map[string]value
}

type rdbFileInfo struct {
	currentDatabaseIndex int
	totalHashCount       int
	hashWithExpiryCount  int
	currentByteCount     int
}

type rdbFileParser struct {
	currentState int
	// currentMode      int
	currentKeyEncoding int
	currentKeyLength   int
	currentKey         string
	currentValueLength int
	currentValue       string
	rdbFileInfo        rdbFileInfo
}

var stateChangeMap = map[int]string{
	startState:               "startState",
	metaDataState:            "metaDataState",
	databaseSectionState:     "databaseSectionState",
	databaseIndexState:       "databaseIndexState",
	totalHashCountState:      "totalHashCountState",
	hashWithExpiryCountState: "hashWithExpiryCountState",
	keyEncodingState:         "keyEncodingState",
	keyLengthState:           "keyLengthState",
	keyParsingState:          "keyParsingState",
	valueLengthState:         "valueLengthState",
	valueParsingState:        "valueParsingState",
	endOfFileState:           "endOfFileState",
}

const (
	startState int = iota
	metaDataState
	databaseSectionState
	databaseIndexState
	totalHashCountState
	hashWithExpiryCountState
	keyEncodingState
	keyLengthState
	keyParsingState
	valueLengthState
	valueParsingState
	endOfFileState
)

// const (
// 	controlMode int = iota
// 	sizeMode
// 	keyValueMode
// )

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

func (r *redisStore) Keys(args []string, config *config) (string, error) {
	if args[0] == "*" {
		path := config.rdb.dir + "/" + config.rdb.dbFileName
		file, err := os.Open(path)
		if err != nil {
			fmt.Println("error opening file", err)
			return "", err
		}

		defer file.Close()
		rdbStore, _ := readFileForKeys(file)
		return allKeysFromRdbStore(rdbStore)

	}
	return "", nil
}

func allKeysFromRdbStore(store rdbStore) (string, error) {
	output := ""
	output += fmt.Sprintf("*%d\r\n", len(store.store))
	for key := range store.store {
		output += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}
	return output, nil
}

func readFileForKeys(file io.Reader) (rdbStore, error) {
	reader := bufio.NewReader(file)
	rdbParser := &rdbFileParser{currentState: startState, rdbFileInfo: rdbFileInfo{}}
	rdbStore := &rdbStore{store: map[string]value{}}

	buffer := make([]byte, 8*1024)

	for {
		rdbValCount, err := reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				fmt.Println("end of buffer")
				break
			}
			fmt.Println("Error while reading byte", err)
		} else {
			rdbParser.rdbFileInfo.currentByteCount += 1
		}

		for i := 0; i < rdbValCount; i++ {
			// fmt.Printf("hex is: %X, string is: %s\n", buffer[i], string(buffer[i]))
			currentState := rdbParser.currentState

			rdbParser = getNextState(rdbParser, buffer, &i, rdbStore)
			if currentState != rdbParser.currentState {
				// fmt.Printf("State changes from current %s -> next %s\n", stateChangeMap[currentState], stateChangeMap[rdbParser.currentState])
			}
		}
	}
	return *rdbStore, nil
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

func getNextState(rdbParser *rdbFileParser, buffer []byte, i *int, rdbStore *rdbStore) *rdbFileParser {
	b := buffer[*i]
	switch rdbParser.currentState {
	case startState:
		if b == 0xFA {
			rdbParser.currentState = metaDataState
		}
	case metaDataState:
		if b == 0xFE {
			rdbParser.currentState = databaseSectionState
		}
	case databaseSectionState:
		rdbParser.rdbFileInfo.currentDatabaseIndex = int(b)
		rdbParser.currentState = databaseIndexState
	case databaseIndexState:
		if b == 0xFB {
			rdbParser.currentState = totalHashCountState
		}
	case totalHashCountState:
		rdbParser.rdbFileInfo.totalHashCount = int(b)
		rdbParser.currentState = hashWithExpiryCountState
	case hashWithExpiryCountState:
		rdbParser.rdbFileInfo.hashWithExpiryCount = int(b)
		rdbParser.currentState = keyEncodingState
	case keyEncodingState:
		rdbParser.currentKeyEncoding = int(b)
		rdbParser.currentState = keyLengthState
	case keyLengthState:
		rdbParser.currentKeyLength = int(b)
		rdbParser.currentState = keyParsingState
	case keyParsingState:
		rdbParser.currentKey = getBufferValue(rdbParser.currentKeyLength, buffer, i)
		rdbParser.currentState = valueLengthState
	case valueLengthState:
		rdbParser.currentValueLength = int(b)
		rdbParser.currentState = valueParsingState
	case valueParsingState:
		if buffer[*i] == 0xFF {
			rdbParser.currentState = endOfFileState
			rdbStore.store[rdbParser.currentKey] = value{
				content: rdbParser.currentValue,
				expiry:  0,
			}
			rdbParser.currentKey = ""
			rdbParser.currentValue = ""
			break
		}

		rdbParser.currentValue = getBufferValue(rdbParser.currentValueLength, buffer, i)
	}
	return rdbParser
}

func getBufferValue(keyLength int, buffer []byte, i *int) string {
	if *i+keyLength < len(buffer) {
		currentKey := string(buffer[*i : *i+keyLength])
		*i += keyLength - 1
		return currentKey
	} else {
		// For now.
		return ""
	}
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
	case "keys":
		return store.Keys(args, config)
	default:
		return "", errors.New("err - unknown command")
	}
}
