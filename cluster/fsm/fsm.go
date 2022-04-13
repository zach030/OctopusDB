package fsm

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"sync"

	"github.com/zach030/OctopusDB/internal/cf"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var committedIndexKey = []byte("committed_index_key")

var handlers = make(map[string]func(*pb.LogEntry, engine.Batch) error, 0)

func RegisterHandler(methodName string, handler func(*pb.LogEntry, engine.Batch) error) {
	handlers[methodName] = handler
}

type FSM struct {
	sync.Mutex
	lastCommittedIndex uint64
}

func NewFSM() *FSM {
	value, err := cf.Get(committedIndexKey)
	if err != nil {
		log.Fatalln("get committed index key error", err)
	}
	lastCommittedIndex := uint64(0)
	if len(value) != 0 {
		lastCommittedIndex = binary.BigEndian.Uint64(value)
	}
	log.Printf("last committed index: [%d]", lastCommittedIndex)
	return &FSM{lastCommittedIndex: lastCommittedIndex}
}

func (fsm *FSM) Apply(raftLog *raft.Log) interface{} {
	if fsm.lastCommittedIndex >= raftLog.Index {
		log.Printf("invalid raft log index: [%d], last committed index: [%d]",
			raftLog.Index, fsm.lastCommittedIndex)
		return errors.New("invalid raft log")
	}

	var err error

	batch := cf.NewBatch()
	defer func() {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, raftLog.Index)

		if err != nil {
			batch.Reset()
		}
		_ = batch.Set(committedIndexKey, bs)
		_ = batch.Commit()
		batch.Close()

		fsm.lastCommittedIndex = raftLog.Index
	}()

	logEntry := &pb.LogEntry{}
	err = proto.Unmarshal(raftLog.Data, logEntry)
	if err != nil {
		return err
	}

	log.Printf("apply methodName: [%s], term: [%d], index: [%d]", logEntry.MethodName, raftLog.Term, raftLog.Index)

	handler := handlers[logEntry.MethodName]
	err = handler(logEntry, batch)
	return err
}

func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{lastCommittedIndex: fsm.lastCommittedIndex}, nil
}

func (fsm *FSM) Restore(closer io.ReadCloser) error {
	return closer.Close()
}

type FSMSnapshot struct {
	lastCommittedIndex uint64
}

func (fsmSnapshot *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, fsmSnapshot.lastCommittedIndex)
	_, err := sink.Write(bs)
	return err
}

func (fsmSnapshot *FSMSnapshot) Release() {
}
