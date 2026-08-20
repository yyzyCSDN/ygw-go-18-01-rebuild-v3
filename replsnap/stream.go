package replsnap

import (
	"fmt"
	"io"
)

type Frame struct {
	Data  []byte
	Final bool
}

func ReadFrames(reader io.Reader, blockSize int) ([]Frame, error) {
	if blockSize < 1 {
		return nil, fmt.Errorf("block size must be positive")
	}
	buffer := make([]byte, blockSize)
	frames := make([]Frame, 0)
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			frames = append(frames, Frame{Data: append([]byte(nil), buffer[:count]...), Final: readErr == io.EOF})
		}
		if readErr != nil {
			if readErr == io.EOF {
				return frames, nil
			}
			return nil, readErr
		}
		if count == 0 {
			return nil, io.ErrNoProgress
		}
	}
}
