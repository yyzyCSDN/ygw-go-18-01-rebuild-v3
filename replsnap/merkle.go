package replsnap

import (
	"crypto/sha256"
	"encoding/hex"
)

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func MerkleRoot(chunks [][]byte) string {
	if len(chunks) == 0 {
		return Digest(nil)
	}
	level := make([][]byte, len(chunks))
	for index, chunk := range chunks {
		sum := sha256.Sum256(chunk)
		level[index] = append([]byte(nil), sum[:]...)
	}
	for len(level) > 1 {
		if len(level)%2 == 1 {
			level = append(level, append([]byte(nil), level[len(level)-1]...))
		}
		next := make([][]byte, 0, len(level)/2)
		for index := 0; index < len(level); index += 2 {
			pair := append(append([]byte(nil), level[index]...), level[index+1]...)
			sum := sha256.Sum256(pair)
			next = append(next, append([]byte(nil), sum[:]...))
		}
		level = next
	}
	return hex.EncodeToString(level[0])
}
