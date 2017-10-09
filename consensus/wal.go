package consensus

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	wire "github.com/tendermint/go-wire"
	"github.com/tendermint/tendermint/types"
	auto "github.com/tendermint/tmlibs/autofile"
	cmn "github.com/tendermint/tmlibs/common"
)

//--------------------------------------------------------
// types and functions for savings consensus messages

type TimedWALMessage struct {
	Time time.Time  `json:"time"` // for debugging purposes
	Msg  WALMessage `json:"msg"`
}

type WALMessage interface{}

var _ = wire.RegisterInterface(
	struct{ WALMessage }{},
	wire.ConcreteType{types.EventDataRoundState{}, 0x01},
	wire.ConcreteType{msgInfo{}, 0x02},
	wire.ConcreteType{timeoutInfo{}, 0x03},
)

//--------------------------------------------------------
// Simple write-ahead logger

// Write ahead logger writes msgs to disk before they are processed.
// Can be used for crash-recovery and deterministic replay
// TODO: currently the wal is overwritten during replay catchup
//   give it a mode so it's either reading or appending - must read to end to start appending again
type WAL struct {
	cmn.BaseService

	group *auto.Group
	light bool // ignore block parts
}

func NewWAL(walFile string, light bool) (*WAL, error) {
	group, err := auto.OpenGroup(walFile)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		group: group,
		light: light,
	}
	wal.BaseService = *cmn.NewBaseService(nil, "WAL", wal)
	return wal, nil
}

func (wal *WAL) OnStart() error {
	size, err := wal.group.Head.Size()
	if err != nil {
		return err
	} else if size == 0 {
		wal.writeEndHeight(0)
	}
	_, err = wal.group.Start()
	return err
}

func (wal *WAL) OnStop() {
	wal.BaseService.OnStop()
	wal.group.Stop()
}

// called in newStep and for each pass in receiveRoutine
func (wal *WAL) Save(wmsg WALMessage) {
	if wal == nil {
		return
	}

	if wal.light {
		// in light mode we only write new steps, timeouts, and our own votes (no proposals, block parts)
		if mi, ok := wmsg.(msgInfo); ok {
			if mi.PeerKey != "" {
				return
			}
		}
	}

	// Write the wal message
	wal.save(wire.BinaryBytes(TimedWALMessage{time.Now(), wmsg}))

	// TODO: only flush when necessary
	if err := wal.group.Flush(); err != nil {
		cmn.PanicQ(cmn.Fmt("Error flushing consensus wal buf to file. Error: %v \n", err))
	}
}

func (wal *WAL) writeEndHeight(height int) {
	wal.save([]byte(fmt.Sprintf("#ENDHEIGHT: %v", height)))

	// TODO: only flush when necessary
	if err := wal.group.Flush(); err != nil {
		cmn.PanicQ(cmn.Fmt("Error flushing consensus wal buf to file. Error: %v \n", err))
	}
}

// save prepends msg with checksum and length header, then writes msg to WAL.
// It panics if write fails or number of bytes written is less than given.
func (wal *WAL) save(msg []byte) {
	crc := crc32.Checksum(msg, crc32c)
	length := uint32(len(msg))
	totalLength := 8 + int(length)

	bytes := make([]byte, totalLength)
	binary.BigEndian.PutUint32(bytes[0:4], crc)
	binary.BigEndian.PutUint32(bytes[4:8], length)
	copy(bytes[8:], msg)

	n, err := wal.group.Write(bytes)
	if err != nil {
		cmn.PanicQ(cmn.Fmt("Error writing msg to consensus wal: %v \n\nMessage: %v", err, msg))
	}
	if n < totalLength {
		cmn.PanicQ(cmn.Fmt("Error writing msg to consensus wal: wanted to write %d bytes, but wrote %d \n\nMessage: %v", totalLength, n, msg))
	}
}
