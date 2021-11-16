package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	basePath := conf.DBPath
	kvPath := basePath + "/kv"
	raftPath := basePath + "/raft"
	isRaft := conf.Raft

	kvEngine := engine_util.CreateDB(kvPath, isRaft)
	raftEngine := engine_util.CreateDB(raftPath, isRaft)

	standAloneEngine := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	return &StandAloneStorage{
		engines: standAloneEngine,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engines.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandAloneStorageReader{
		txn: s.engines.Kv.NewTransaction(false),
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			put.Cf = b.Cf()
			put.Key = b.Key()
			put.Value = b.Value()
			err := engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value)

			if err != nil {
				return err
			}
		case storage.Delete:
			del := b.Data.(storage.Delete)
			del.Cf = b.Cf()
			del.Key = b.Key()
			err := engine_util.DeleteCF(s.engines.Kv, del.Cf, del.Key)

			if err != nil {
				return err
			}
		}
	}
	return nil
}

// StandAloneStorageReader

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return value, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, s.txn)
	return iter
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
