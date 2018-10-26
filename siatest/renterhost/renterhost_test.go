package renterhost

import (
	"bytes"
	"path/filepath"
	"testing"

	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/proto"
	"gitlab.com/NebulousLabs/Sia/siatest"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
)

type stubHostDB struct{}

func (stubHostDB) IncrementSuccessfulInteractions(types.SiaPublicKey) {}
func (stubHostDB) IncrementFailedInteractions(types.SiaPublicKey)     {}

// TestSession tests the new RPC loop by creating a host and requesting new
// RPCs via the proto.Session type.
func TestSession(t *testing.T) {
	gp := siatest.GroupParams{
		Hosts:   1,
		Renters: 1,
		Miners:  1,
	}
	tg, err := siatest.NewGroupFromTemplate(siatest.TestDir(t.Name()), gp)
	if err != nil {
		t.Fatal(err)
	}
	defer tg.Close()

	// manually grab a renter contract
	renter := tg.Renters()[0]
	cs, err := proto.NewContractSet(filepath.Join(renter.Dir, "renter", "contracts"), new(modules.ProductionDependencies))
	if err != nil {
		t.Fatal(err)
	}
	contract := cs.ViewAll()[0]

	hhg, err := renter.HostDbHostsGet(contract.HostPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cg, err := renter.ConsensusGet()
	if err != nil {
		t.Fatal(err)
	}

	// begin the RPC session
	s, err := cs.NewSession(hhg.Entry.HostDBEntry, contract.ID, cg.Height, stubHostDB{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// upload a sector
	sector := fastrand.Bytes(int(modules.SectorSize))
	_, root, err := s.Upload(sector)
	if err != nil {
		t.Fatal(err)
	}

	// download the sector
	_, dsector, err := s.Download(modules.LoopDownloadRequest{
		MerkleRoot: root,
		Length:     uint32(len(sector)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sector, dsector) {
		t.Fatal("downloaded sector does not match")
	}
}