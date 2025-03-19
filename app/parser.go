package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Example input - *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
func parseRESPString(reader *bufio.Reader) (string, []string, error) {

	header, _, err := reader.ReadLine()
	if err != nil {
		return "", nil, err
	}
	fmt.Println("Header is", string(header[:]))
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

			args = append(args, strings.ToLower(string(stringBytes)))
		}
	}

	return command, args, nil
}

// func parseRESPSetForReplica(reader *bufio.Reader) (string, []string, error) {

// }

func parseRESPInteger(s string, min int, errorFormat string) (int, error) {
	val, err := strconv.Atoi(s)
	if err != nil || val < min {
		return 0, fmt.Errorf(errorFormat, s)
	}
	return val, nil
}

func respArrayGenerator(key, output string) string {
	respArray := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(output), output)
	return respArray
}

func respGenerator(args []string) string {
	// '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n'
	output := fmt.Sprintf("*%d\r\n", len(args))
	for _, v := range args {
		output += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
	}
	return output
}
