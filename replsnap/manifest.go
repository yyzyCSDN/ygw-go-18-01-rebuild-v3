package replsnap

import "fmt"

func BuildManifest(snapshotID string, frames []Frame) (Manifest, [][]byte, error) {
	if snapshotID == "" || len(frames) == 0 {
		return Manifest{}, nil, fmt.Errorf("%w: empty snapshot", ErrInvalidManifest)
	}
	chunks := make([][]byte, 0, len(frames))
	metadata := make([]ChunkMeta, 0, len(frames))
	for index, frame := range frames {
		if len(frame.Data) == 0 {
			return Manifest{}, nil, fmt.Errorf("%w: empty chunk %d", ErrInvalidManifest, index)
		}
		chunk := append([]byte(nil), frame.Data...)
		chunks = append(chunks, chunk)
		metadata = append(metadata, ChunkMeta{Index: index, Size: len(chunk), Digest: Digest(chunk), Final: frame.Final})
	}
	return Manifest{SnapshotID: snapshotID, Chunks: metadata, MerkleRoot: MerkleRoot(chunks)}, chunks, nil
}
