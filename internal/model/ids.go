package model

import (
	"fmt"
	"sync/atomic"
)

var nextID atomic.Uint64

func NewID(prefix string) string {
	return fmt.Sprintf("%s-%08d", prefix, nextID.Add(1))
}
