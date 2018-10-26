package siafile

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/fastrand"
)

// TestNumChunkPagesRequired tests numChunkPagesRequired.
func TestNumChunkPagesRequired(t *testing.T) {
	for numPieces := 0; numPieces < 1000; numPieces++ {
		chunkSize := marshaledChunkSize(numPieces)
		expectedPages := int8(chunkSize/pageSize + 1)
		if numChunkPagesRequired(numPieces) != expectedPages {
			t.Fatalf("expected %v pages but got %v", expectedPages, numChunkPagesRequired(numPieces))
		}
	}
}

// TestMarshalUnmarshalChunk tests marshaling and unmarshaling a single chunk.
func TestMarshalUnmarshalChunk(t *testing.T) {
	// Get random chunk.
	chunk := randomChunk()
	numPieces := uint32(len(chunk.Pieces))

	// Marshal the chunk.
	chunkBytes, err := marshalChunk(chunk)
	if err != nil {
		t.Fatal(err)
	}
	// Check the length of the marshaled chunk.
	if int64(len(chunkBytes)) != marshaledChunkSize(chunk.numPieces()) {
		t.Fatalf("ChunkBytes should have len %v but was %v",
			marshaledChunkSize(chunk.numPieces()), len(chunkBytes))
	}
	// Add some random bytes to the chunkBytes. It should be possible to
	// unmarshal chunks even if we pass in data that is padded at the end.
	chunkBytes = append(chunkBytes, fastrand.Bytes(100)...)

	// Unmarshal the chunk.
	unmarshaledChunk, err := unmarshalChunk(numPieces, chunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	// Compare unmarshaled chunk to original.
	if !reflect.DeepEqual(chunk, unmarshaledChunk) {
		t.Log("original", chunk)
		t.Log("unmarshaled", unmarshaledChunk)
		t.Fatal("Unmarshaled chunk doesn't equal marshaled chunk")
	}
}

// TestMarshalUnmarshalErasureCoder tests marshaling and unmarshaling an
// ErasureCoder.
func TestMarshalUnmarshalErasureCoder(t *testing.T) {
	rc, err := NewRSCode(10, 20)
	if err != nil {
		t.Fatal("failed to create reed solomon coder", err)
	}
	// Get the minimum pieces and the total number of pieces.
	numPieces, minPieces := rc.NumPieces(), rc.MinPieces()
	// Marshal the erasure coder.
	ecType, ecParams := marshalErasureCoder(rc)
	// Unmarshal it.
	rc2, err := unmarshalErasureCoder(ecType, ecParams)
	if err != nil {
		t.Fatal("failed to unmarshal reed solomon coder", err)
	}
	// Check if the settings are still the same.
	if numPieces != rc2.NumPieces() {
		t.Errorf("expected %v numPieces but was %v", numPieces, rc2.NumPieces())
	}
	if minPieces != rc2.MinPieces() {
		t.Errorf("expected %v minPieces but was %v", minPieces, rc2.MinPieces())
	}
}

// TestMarshalUnmarshalMetadata tests marshaling and unmarshaling the metadata
// of a SiaFile.
func TestMarshalUnmarshalMetadata(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()

	// Marshal metadata
	raw, err := marshalMetadata(sf.staticMetadata)
	if err != nil {
		t.Fatal("Failed to marshal metadata", err)
	}
	// Unmarshal metadata
	md, err := unmarshalMetadata(raw)
	if err != nil {
		t.Fatal("Failed to unmarshal metadata", err)
	}
	// Compare the timestamps first since they can't be compared with
	// DeepEqual.
	if sf.staticMetadata.AccessTime.Unix() != md.AccessTime.Unix() {
		t.Fatal("AccessTime's don't match")
	}
	if sf.staticMetadata.ChangeTime.Unix() != md.ChangeTime.Unix() {
		t.Fatal("ChangeTime's don't match")
	}
	if sf.staticMetadata.CreateTime.Unix() != md.CreateTime.Unix() {
		t.Fatal("CreateTime's don't match")
	}
	if sf.staticMetadata.ModTime.Unix() != md.ModTime.Unix() {
		t.Fatal("ModTime's don't match")
	}
	// Set the timestamps to zero for DeepEqual.
	sf.staticMetadata.AccessTime = time.Time{}
	sf.staticMetadata.ChangeTime = time.Time{}
	sf.staticMetadata.CreateTime = time.Time{}
	sf.staticMetadata.ModTime = time.Time{}
	md.AccessTime = time.Time{}
	md.ChangeTime = time.Time{}
	md.CreateTime = time.Time{}
	md.ModTime = time.Time{}
	// Compare result to original
	if !reflect.DeepEqual(md, sf.staticMetadata) {
		t.Fatal("Unmarshaled metadata not equal to marshaled metadata:", err)
	}
}

// TestMarshalUnmarshalPubKeyTable tests marshaling and unmarshaling the
// publicKeyTable of a SiaFile.
func TestMarshalUnmarshalPubKeyTable(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()
	sf.addRandomHostKeys(10)

	// Marshal pubKeyTable.
	raw, err := marshalPubKeyTable(sf.pubKeyTable)
	if err != nil {
		t.Fatal("Failed to marshal pubKeyTable", err)
	}
	// Unmarshal pubKeyTable.
	pubKeyTable, err := unmarshalPubKeyTable(raw)
	if err != nil {
		t.Fatal("Failed to unmarshal pubKeyTable", err)
	}
	// Compare them.
	if len(sf.pubKeyTable) != len(pubKeyTable) {
		t.Fatalf("Lengths of tables don't match %v vs %v",
			len(sf.pubKeyTable), len(pubKeyTable))
	}
	for i, spk := range pubKeyTable {
		if spk.Used != sf.pubKeyTable[i].Used {
			t.Fatal("Use fields don't match")
		}
		if spk.PublicKey.Algorithm != sf.pubKeyTable[i].PublicKey.Algorithm {
			t.Fatal("Algorithms don't match")
		}
		if !bytes.Equal(spk.PublicKey.Key, sf.pubKeyTable[i].PublicKey.Key) {
			t.Fatal("Keys don't match")
		}
	}
}

// TestMarshalUnmarshalPiece tests marshaling and unmarshaling a single piece
// of a chunk.
func TestMarshalUnmarshalPiece(t *testing.T) {
	// Create random piece.
	piece := randomPiece()
	pieceIndex := uint32(fastrand.Intn(100))

	// Marshal the piece.
	rawPiece, err := marshalPiece(nil, pieceIndex, piece)
	if err != nil {
		t.Fatal(err)
	}
	// Check the length.
	if len(rawPiece) != marshaledPieceSize {
		t.Fatalf("Expected marshaled piece to have length %v but was %v",
			marshaledPieceSize, len(rawPiece))
	}
	// Unmarshal the piece.
	unmarshaledPieceIndex, unmarshaledPiece, err := unmarshalPiece(rawPiece)
	if err != nil {
		t.Fatal(err)
	}
	// Compare the pieceIndex.
	if unmarshaledPieceIndex != pieceIndex {
		t.Fatalf("Piece index should be %v but was %v", pieceIndex, unmarshaledPieceIndex)
	}
	// Compare the piece to the original.
	if !reflect.DeepEqual(unmarshaledPiece, piece) {
		t.Fatal("Piece doesn't equal unmarshaled piece")
	}

	// Marshal the piece again but this time append it to the existing piece.
	twoRawPieces, err := marshalPiece(rawPiece, pieceIndex, piece)
	if err != nil {
		t.Fatal(err)
	}
	// Check the length.
	if len(twoRawPieces) != 2*marshaledPieceSize {
		t.Fatalf("Expected marshaled pieces to have length %v but was %v",
			2*marshaledPieceSize, len(rawPiece))
	}
	// Unmarshal both pieces.
	unmarshaledPieceIndex1, unmarshaledPiece1, err := unmarshalPiece(twoRawPieces[:marshaledPieceSize])
	if err != nil {
		t.Fatal(err)
	}
	unmarshaledPieceIndex2, unmarshaledPiece2, err := unmarshalPiece(twoRawPieces[marshaledPieceSize:])
	if err != nil {
		t.Fatal(err)
	}
	// They should be the same and also equal to piece.
	if !reflect.DeepEqual(unmarshaledPiece1, unmarshaledPiece2) {
		t.Fatal("Unmarshaled pieces don't match")
	}
	if !reflect.DeepEqual(unmarshaledPiece1, piece) {
		t.Fatal("Unmarshaled pieces don't match original piece")
	}
	// Their index should be the same and equal to pieceIndex.
	if pieceIndex != unmarshaledPieceIndex1 || unmarshaledPieceIndex1 != unmarshaledPieceIndex2 {
		t.Fatal("Piece indices don't match")
	}

	// Marshal the piece again but this time use already allocated memory.
	rawPiece = make([]byte, 0, 2*marshaledPieceSize) // enough memory for 2 pieces.
	rawPieceBefore := rawPiece
	rawPiece, err = marshalPiece(rawPiece, pieceIndex, piece)
	if err != nil {
		t.Fatal(err)
	}
	// Check the length.
	if len(rawPiece) != marshaledPieceSize {
		t.Fatalf("Expected marshaled piece to have length %v but was %v",
			marshaledPieceSize, len(rawPiece))
	}
	// Unmarshal the piece.
	unmarshaledPieceIndex, unmarshaledPiece, err = unmarshalPiece(rawPiece)
	if err != nil {
		t.Fatal(err)
	}
	// Compare the pieceIndex.
	if unmarshaledPieceIndex != pieceIndex {
		t.Fatalf("Piece index should be %v but was %v", pieceIndex, unmarshaledPieceIndex)
	}
	// Compare the piece to the original.
	if !reflect.DeepEqual(unmarshaledPiece, piece) {
		t.Fatal("Piece doesn't equal unmarshaled piece")
	}
	// Make sure that rawPiece uses the same address in memory as before.
	rawPiece = rawPiece[:cap(rawPiece)]
	rawPieceBefore = rawPieceBefore[:cap(rawPieceBefore)]
	if &rawPiece[cap(rawPiece)-1] != &rawPieceBefore[cap(rawPieceBefore)-1] {
		t.Fatal("rawPiece doesn't use the same memory as rawPieceBefore")
	}
}
