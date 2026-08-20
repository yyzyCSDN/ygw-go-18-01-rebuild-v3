package replsnap

import "fmt"

func VerifyManifest(manifest Manifest, chunks [][]byte) error {
	if manifest.SnapshotID == "" || len(manifest.Chunks) != len(chunks) || len(chunks) == 0 {
		return fmt.Errorf("%w: chunk count", ErrInvalidManifest)
	}
	for index, chunk := range chunks {
		meta := manifest.Chunks[index]
		if meta.Index != index || meta.Size != len(chunk) || meta.Digest != Digest(chunk) {
			return fmt.Errorf("%w: chunk %d", ErrInvalidManifest, index)
		}
	}
	if manifest.MerkleRoot != MerkleRoot(chunks) {
		return fmt.Errorf("%w: merkle root", ErrInvalidManifest)
	}
	return nil
}
