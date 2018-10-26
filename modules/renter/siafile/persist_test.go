package siafile

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/NebulousLabs/writeaheadlog"
)

// addRandomHostKeys adds n random host keys to the SiaFile's pubKeyTable. It
// doesn't write them to disk.
func (sf *SiaFile) addRandomHostKeys(n int) {
	for i := 0; i < n; i++ {
		// Create random specifier and key.
		algorithm := types.Specifier{}
		fastrand.Read(algorithm[:])

		// Create random key.
		key := fastrand.Bytes(32)

		// Append new key to slice.
		sf.pubKeyTable = append(sf.pubKeyTable, HostPublicKey{
			PublicKey: types.SiaPublicKey{
				Algorithm: algorithm,
				Key:       key,
			},
			Used: true,
		})
	}
}

// newBlankTestFile is a helper method to create a SiaFile for testing without
// any hosts or uploaded pieces.
func newBlankTestFile() *SiaFile {
	// Create arguments for new file.
	sk := crypto.GenerateSiaKey(crypto.RandomCipherType())
	pieceSize := modules.SectorSize - sk.Type().Overhead()
	siaPath := string(hex.EncodeToString(fastrand.Bytes(8)))
	rc, err := NewRSCode(10, 20)
	if err != nil {
		panic(err)
	}
	numChunks := fastrand.Intn(10) + 1
	fileSize := pieceSize * uint64(rc.MinPieces()) * uint64(numChunks)
	fileMode := os.FileMode(777)
	source := string(hex.EncodeToString(fastrand.Bytes(8)))

	// Create the path to the file.
	siaFilePath := filepath.Join(os.TempDir(), "siafiles", siaPath)
	dir, _ := filepath.Split(siaFilePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	// Create the file.
	sf, err := New(siaFilePath, siaPath, source, newTestWAL(), rc, sk, fileSize, fileMode)
	if err != nil {
		panic(err)
	}
	// Check that the number of chunks in the file is correct.
	if len(sf.staticChunks) != numChunks {
		panic("newTestFile didn't create the expected number of chunks")
	}
	return sf
}

// newTestFile creates a SiaFile for testing where each chunk has a random
// number of pieces.
func newTestFile() *SiaFile {
	sf := newBlankTestFile()
	// Add pieces to each chunk.
	for chunkIndex := range sf.staticChunks {
		for pieceIndex := 0; pieceIndex < sf.ErasureCode().NumPieces(); pieceIndex++ {
			numPieces := fastrand.Intn(3) // up to 2 hosts for each piece
			for i := 0; i < numPieces; i++ {
				pk := types.SiaPublicKey{Key: fastrand.Bytes(crypto.EntropySize)}
				mr := crypto.Hash{}
				fastrand.Read(mr[:])
				if err := sf.AddPiece(pk, uint64(chunkIndex), uint64(pieceIndex), mr); err != nil {
					panic(err)
				}
			}
		}
	}
	return sf
}

// newTestWal is a helper method to create a WAL for testing.
func newTestWAL() *writeaheadlog.WAL {
	// Create the wal.
	walsDir := filepath.Join(os.TempDir(), "wals")
	if err := os.MkdirAll(walsDir, 0700); err != nil {
		panic(err)
	}
	walFilePath := filepath.Join(walsDir, hex.EncodeToString(fastrand.Bytes(8)))
	_, wal, err := writeaheadlog.New(walFilePath)
	if err != nil {
		panic(err)
	}
	return wal
}

// TestNewFile tests that a new file has the correct contents and size and that
// loading it from disk also works.
func TestNewFile(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()
	sf := newTestFile()

	// Check that StaticPagesPerChunk was set correctly.
	if sf.staticMetadata.StaticPagesPerChunk != numChunkPagesRequired(sf.staticMetadata.staticErasureCode.NumPieces()) {
		t.Fatal("StaticPagesPerChunk wasn't set correctly")
	}

	// Marshal the metadata.
	md, err := marshalMetadata(sf.staticMetadata)
	if err != nil {
		t.Fatal(err)
	}
	// Marshal the pubKeyTable.
	pkt, err := marshalPubKeyTable(sf.pubKeyTable)
	if err != nil {
		t.Fatal(err)
	}
	// Marshal the chunks.
	var chunks [][]byte
	for _, chunk := range sf.staticChunks {
		c, err := marshalChunk(chunk)
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
	}

	// Open the file.
	f, err := os.OpenFile(sf.siaFilePath, os.O_RDWR, 777)
	if err != nil {
		t.Fatal("Failed to open file", err)
	}
	defer f.Close()
	// Check the filesize. It should be equal to the offset of the last chunk
	// on disk + its marshaled length.
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != sf.chunkOffset(len(sf.staticChunks)-1)+int64(len(chunks[len(chunks)-1])) {
		t.Fatal("file doesn't have right size")
	}
	// Compare the metadata to the on-disk metadata.
	readMD := make([]byte, len(md))
	_, err = f.ReadAt(readMD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readMD, md) {
		t.Fatal("metadata doesn't equal on-disk metadata")
	}
	// Compare the pubKeyTable to the on-disk pubKeyTable.
	readPKT := make([]byte, len(pkt))
	_, err = f.ReadAt(readPKT, sf.staticMetadata.PubKeyTableOffset)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readPKT, pkt) {
		t.Fatal("pubKeyTable doesn't equal on-disk pubKeyTable")
	}
	// Compare the chunks to the on-disk chunks one-by-one.
	readChunk := make([]byte, int(sf.staticMetadata.StaticPagesPerChunk)*pageSize)
	for chunkIndex := range sf.staticChunks {
		_, err := f.ReadAt(readChunk, sf.chunkOffset(chunkIndex))
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !bytes.Equal(readChunk[:len(chunks[chunkIndex])], chunks[chunkIndex]) {
			t.Fatal("readChunks don't equal on-disk readChunks")
		}
	}
	// Load the file from disk and check that they are the same.
	sf2, err := LoadSiaFile(sf.siaFilePath, sf.wal)
	if err != nil {
		t.Fatal("failed to load SiaFile from disk", err)
	}
	// Compare the timestamps first since they can't be compared with
	// DeepEqual.
	if sf.staticMetadata.AccessTime.Unix() != sf2.staticMetadata.AccessTime.Unix() {
		t.Fatal("AccessTime's don't match")
	}
	if sf.staticMetadata.ChangeTime.Unix() != sf2.staticMetadata.ChangeTime.Unix() {
		t.Fatal("ChangeTime's don't match")
	}
	if sf.staticMetadata.CreateTime.Unix() != sf2.staticMetadata.CreateTime.Unix() {
		t.Fatal("CreateTime's don't match")
	}
	if sf.staticMetadata.ModTime.Unix() != sf2.staticMetadata.ModTime.Unix() {
		t.Fatal("ModTime's don't match")
	}
	// Set the timestamps to zero for DeepEqual.
	sf.staticMetadata.AccessTime = time.Time{}
	sf.staticMetadata.ChangeTime = time.Time{}
	sf.staticMetadata.CreateTime = time.Time{}
	sf.staticMetadata.ModTime = time.Time{}
	sf2.staticMetadata.AccessTime = time.Time{}
	sf2.staticMetadata.ChangeTime = time.Time{}
	sf2.staticMetadata.CreateTime = time.Time{}
	sf2.staticMetadata.ModTime = time.Time{}
	// Compare the rest of sf and sf2.
	if !reflect.DeepEqual(sf.staticMetadata, sf2.staticMetadata) {
		fmt.Println(sf.staticMetadata)
		fmt.Println(sf2.staticMetadata)
		t.Error("sf metadata doesn't equal sf2 metadata")
	}
	if !reflect.DeepEqual(sf.pubKeyTable, sf2.pubKeyTable) {
		t.Log(sf.pubKeyTable)
		t.Log(sf2.pubKeyTable)
		t.Error("sf pubKeyTable doesn't equal sf2 pubKeyTable")
	}
	if !reflect.DeepEqual(sf.staticChunks, sf2.staticChunks) {
		t.Error("sf chunks don't equal sf2 chunks")
	}
	if sf.siaFilePath != sf2.siaFilePath {
		t.Errorf("sf2 filepath was %v but should be %v",
			sf2.siaFilePath, sf.siaFilePath)
	}
}

// TestCreateReadInsertUpdate tests if an update can be created using createInsertUpdate
// and if the created update can be read using readInsertUpdate.
func TestCreateReadInsertUpdate(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()
	// Create update randomly
	index := int64(fastrand.Intn(100))
	data := fastrand.Bytes(10)
	update := sf.createInsertUpdate(index, data)
	// Read update
	readPath, readIndex, readData, err := readInsertUpdate(update)
	if err != nil {
		t.Fatal("Failed to read update", err)
	}
	// Compare values
	if readPath != sf.siaFilePath {
		t.Error("paths doesn't match")
	}
	if readIndex != index {
		t.Error("index doesn't match")
	}
	if !bytes.Equal(readData, data) {
		t.Error("data doesn't match")
	}
}

// TestCreateReadDeleteUpdate tests if an update can be created using
// createDeleteUpdate and if the created update can be read using
// readDeleteUpdate.
func TestCreateReadDeleteUpdate(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()
	update := sf.createDeleteUpdate()
	// Read update
	path := readDeleteUpdate(update)
	// Compare values
	if path != sf.siaFilePath {
		t.Error("paths doesn't match")
	}
}

// TestDelete tests if deleting a siafile removes the file from disk and sets
// the deleted flag correctly.
func TestDelete(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()
	// Delete file.
	if err := sf.Delete(); err != nil {
		t.Fatal("Failed to delete file", err)
	}
	// Check if file was deleted and if deleted flag was set.
	if !sf.Deleted() {
		t.Fatal("Deleted flag was not set correctly")
	}
	if _, err := os.Open(sf.siaFilePath); !os.IsNotExist(err) {
		t.Fatal("Expected a file doesn't exist error but got", err)
	}
}

// TestRename tests if renaming a siafile moves the file correctly and also
// updates the metadata.
func TestRename(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()

	// Create new paths for the file.
	newSiaPath := sf.staticMetadata.SiaPath + "1"
	newSiaFilePath := sf.siaFilePath + "1"
	oldSiaFilePath := sf.siaFilePath

	// Rename file
	if err := sf.Rename(newSiaPath, newSiaFilePath); err != nil {
		t.Fatal("Failed to rename file", err)
	}

	// Check if the file was moved.
	if _, err := os.Open(oldSiaFilePath); !os.IsNotExist(err) {
		t.Fatal("Expected a file doesn't exist error but got", err)
	}
	f, err := os.Open(newSiaFilePath)
	if err != nil {
		t.Fatal("Failed to open file at new location", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Check the metadata.
	if sf.siaFilePath != newSiaFilePath {
		t.Fatal("SiaFilePath wasn't updated correctly")
	}
	if sf.staticMetadata.SiaPath != newSiaPath {
		t.Fatal("SiaPath wasn't updated correctly")
	}
}

// TestApplyUpdates tests a variety of functions that are used to apply
// updates.
func TestApplyUpdates(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	t.Run("TestApplyUpdates", func(t *testing.T) {
		siaFile := newTestFile()
		testApply(t, siaFile, ApplyUpdates)
	})
	t.Run("TestSiaFileApplyUpdates", func(t *testing.T) {
		siaFile := newTestFile()
		testApply(t, siaFile, siaFile.applyUpdates)
	})
	t.Run("TestCreateAndApplyTransaction", func(t *testing.T) {
		siaFile := newTestFile()
		testApply(t, siaFile, siaFile.createAndApplyTransaction)
	})
}

// TestSaveSmallHeader tests the saveHeader method for a header that is not big
// enough to need more than a single page on disk.
func TestSaveSmallHeader(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newBlankTestFile()

	// Add some host keys.
	sf.addRandomHostKeys(10)

	// Save the header.
	updates, err := sf.saveHeader()
	if err != nil {
		t.Fatal("Failed to create updates to save header", err)
	}
	if err := sf.createAndApplyTransaction(updates...); err != nil {
		t.Fatal("Failed to save header", err)
	}

	// Manually open the file to check its contents.
	f, err := os.Open(sf.siaFilePath)
	if err != nil {
		t.Fatal("Failed to open file", err)
	}
	defer f.Close()

	// Make sure the metadata was written to disk correctly.
	rawMetadata, err := marshalMetadata(sf.staticMetadata)
	if err != nil {
		t.Fatal("Failed to marshal metadata", err)
	}
	readMetadata := make([]byte, len(rawMetadata))
	if _, err := f.ReadAt(readMetadata, 0); err != nil {
		t.Fatal("Failed to read metadata", err)
	}
	if !bytes.Equal(rawMetadata, readMetadata) {
		fmt.Println(string(rawMetadata))
		fmt.Println(string(readMetadata))
		t.Fatal("Metadata on disk doesn't match marshaled metadata")
	}

	// Make sure that the pubKeyTable was written to disk correctly.
	rawPubKeyTAble, err := marshalPubKeyTable(sf.pubKeyTable)
	if err != nil {
		t.Fatal("Failed to marshal pubKeyTable", err)
	}
	readPubKeyTable := make([]byte, len(rawPubKeyTAble))
	if _, err := f.ReadAt(readPubKeyTable, sf.staticMetadata.PubKeyTableOffset); err != nil {
		t.Fatal("Failed to read pubKeyTable", err)
	}
	if !bytes.Equal(rawPubKeyTAble, readPubKeyTable) {
		t.Fatal("pubKeyTable on disk doesn't match marshaled pubKeyTable")
	}
}

// TestSaveLargeHeader tests the saveHeader method for a header that uses more than a single page on disk and forces a call to allocateHeaderPage
func TestSaveLargeHeader(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newBlankTestFile()

	// Add some host keys. This should force the SiaFile to allocate a new page
	// for the pubKeyTable.
	sf.addRandomHostKeys(100)

	// Open the file.
	f, err := os.OpenFile(sf.siaFilePath, os.O_RDWR, 777)
	if err != nil {
		t.Fatal("Failed to open file", err)
	}
	defer f.Close()

	// Write some data right after the ChunkOffset as a checksum.
	chunkData := fastrand.Bytes(100)
	_, err = f.WriteAt(chunkData, sf.staticMetadata.ChunkOffset)
	if err != nil {
		t.Fatal("Failed to write random chunk data", err)
	}

	// Save the header.
	updates, err := sf.saveHeader()
	if err != nil {
		t.Fatal("Failed to create updates to save header", err)
	}
	if err := sf.createAndApplyTransaction(updates...); err != nil {
		t.Fatal("Failed to save header", err)
	}

	// Make sure the chunkOffset was updated correctly.
	if sf.staticMetadata.ChunkOffset != 2*pageSize {
		t.Fatal("ChunkOffset wasn't updated correctly", sf.staticMetadata.ChunkOffset, 2*pageSize)
	}

	// Make sure that the checksum was moved correctly.
	readChunkData := make([]byte, len(chunkData))
	if _, err := f.ReadAt(readChunkData, sf.staticMetadata.ChunkOffset); err != nil {
		t.Fatal("Checksum wasn't moved correctly")
	}

	// Make sure the metadata was written to disk correctly.
	rawMetadata, err := marshalMetadata(sf.staticMetadata)
	if err != nil {
		t.Fatal("Failed to marshal metadata", err)
	}
	readMetadata := make([]byte, len(rawMetadata))
	if _, err := f.ReadAt(readMetadata, 0); err != nil {
		t.Fatal("Failed to read metadata", err)
	}
	if !bytes.Equal(rawMetadata, readMetadata) {
		fmt.Println(string(rawMetadata))
		fmt.Println(string(readMetadata))
		t.Fatal("Metadata on disk doesn't match marshaled metadata")
	}

	// Make sure that the pubKeyTable was written to disk correctly.
	rawPubKeyTAble, err := marshalPubKeyTable(sf.pubKeyTable)
	if err != nil {
		t.Fatal("Failed to marshal pubKeyTable", err)
	}
	readPubKeyTable := make([]byte, len(rawPubKeyTAble))
	if _, err := f.ReadAt(readPubKeyTable, sf.staticMetadata.PubKeyTableOffset); err != nil {
		t.Fatal("Failed to read pubKeyTable", err)
	}
	if !bytes.Equal(rawPubKeyTAble, readPubKeyTable) {
		t.Fatal("pubKeyTable on disk doesn't match marshaled pubKeyTable")
	}
}

// testApply tests if a given method applies a set of updates correctly.
func testApply(t *testing.T, siaFile *SiaFile, apply func(...writeaheadlog.Update) error) {
	// Create an update that writes random data to a random index i.
	index := fastrand.Intn(100) + 1
	data := fastrand.Bytes(100)
	update := siaFile.createInsertUpdate(int64(index), data)

	// Apply update.
	if err := apply(update); err != nil {
		t.Fatal("Failed to apply update", err)
	}
	// Open file.
	file, err := os.Open(siaFile.siaFilePath)
	if err != nil {
		t.Fatal("Failed to open file", err)
	}
	// Check if correct data was written.
	readData := make([]byte, len(data))
	if _, err := file.ReadAt(readData, int64(index)); err != nil {
		t.Fatal("Failed to read written data back from disk", err)
	}
	if !bytes.Equal(data, readData) {
		t.Fatal("Read data doesn't equal written data")
	}
}

// TestUpdateUsedHosts tests the UpdateUsedHosts method.
func TestUpdateUsedHosts(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	sf := newTestFile()
	sf.addRandomHostKeys(10)

	// All the host keys should be used.
	for _, entry := range sf.pubKeyTable {
		if !entry.Used {
			t.Fatal("all hosts are expected to be used at the beginning of the test")
		}
	}

	// Report only half the hosts as still being used.
	var used []types.SiaPublicKey
	for i, entry := range sf.pubKeyTable {
		if i%2 == 0 {
			used = append(used, entry.PublicKey)
		}
	}
	if err := sf.UpdateUsedHosts(used); err != nil {
		t.Fatal("failed to update hosts", err)
	}

	// Create a map of the used keys for faster lookups.
	usedMap := make(map[string]struct{})
	for _, key := range used {
		usedMap[string(key.Key)] = struct{}{}
	}

	// Check that the flag was set correctly.
	for _, entry := range sf.pubKeyTable {
		_, exists := usedMap[string(entry.PublicKey.Key)]
		if entry.Used != exists {
			t.Errorf("expected flag to be %v but was %v", exists, entry.Used)
		}
	}

	// Reload the siafile to see if the flags were also persisted.
	var err error
	sf, err = LoadSiaFile(sf.siaFilePath, sf.wal)
	if err != nil {
		t.Fatal(err)
	}

	// Check that the flags are still set correctly.
	for _, entry := range sf.pubKeyTable {
		_, exists := usedMap[string(entry.PublicKey.Key)]
		if entry.Used != exists {
			t.Errorf("expected flag to be %v but was %v", exists, entry.Used)
		}
	}

	// Also check the flags in order. Making sure that persisting them didn't
	// change the order.
	for i, entry := range sf.pubKeyTable {
		expectedUsed := i%2 == 0
		if entry.Used != expectedUsed {
			t.Errorf("expected flag to be %v but was %v", expectedUsed, entry.Used)
		}
	}
}

// TestChunkOffset tests the chunkOffset method.
func TestChunkOffset(t *testing.T) {
	sf := newTestFile()

	// Set the static pages per chunk to a random value.
	sf.staticMetadata.StaticPagesPerChunk = int8(fastrand.Intn(5)) + 1

	// Calculate the offset of the first chunk.
	offset1 := sf.chunkOffset(0)
	if expectedOffset := sf.staticMetadata.ChunkOffset; expectedOffset != offset1 {
		t.Fatalf("expected offset %v but got %v", sf.staticMetadata.ChunkOffset, offset1)
	}

	// Calculate the offset of the second chunk.
	offset2 := sf.chunkOffset(1)
	if expectedOffset := offset1 + int64(sf.staticMetadata.StaticPagesPerChunk)*pageSize; expectedOffset != offset2 {
		t.Fatalf("expected offset %v but got %v", expectedOffset, offset2)
	}

	// Make sure that the offsets we calculated are not the same due to not
	// initializing the file correctly.
	if offset2 == offset1 {
		t.Fatal("the calculated offsets are the same")
	}
}

// TestSaveChunk checks that saveChunk creates an updated which if applied
// writes the correct data to disk.
func TestSaveChunk(t *testing.T) {
	sf := newTestFile()

	// Choose a random chunk from the file and replace it.
	chunkIndex := fastrand.Intn(len(sf.staticChunks))
	chunk := randomChunk()
	sf.staticChunks[chunkIndex] = chunk

	// Write the chunk to disk using saveChunk.
	update, err := sf.saveChunk(chunkIndex)
	if err != nil {
		t.Fatal(err)
	}
	if err := sf.createAndApplyTransaction(update); err != nil {
		t.Fatal(err)
	}

	// Marshal the chunk.
	marshaledChunk, err := marshalChunk(chunk)
	if err != nil {
		t.Fatal(err)
	}

	// Read the chunk from disk.
	f, err := os.Open(sf.siaFilePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	readChunk := make([]byte, len(marshaledChunk))
	if _, err := f.ReadAt(readChunk, sf.chunkOffset(chunkIndex)); err != nil {
		t.Fatal(err)
	}

	// The marshaled chunk should equal the chunk we read from disk.
	if !bytes.Equal(readChunk, marshaledChunk) {
		t.Fatal("marshaled chunk doesn't equal chunk on disk")
	}
}
