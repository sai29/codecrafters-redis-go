package main

import "time"

func expired(expiryTime int64) bool {
	return time.Now().UnixNano() >= expiryTime
}
