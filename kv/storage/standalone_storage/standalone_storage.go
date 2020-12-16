package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	var (
		kvTxn = s.engine.Kv.NewTransaction(false)
		raftTxn = s.engine.Kv.NewTransaction(false)
	)
	return &StandaloneReader{
		kvTxn:   kvTxn,
		raftTxn: raftTxn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			put := modify.Data.(storage.Put)
			var txn *badger.Txn
			if put.Cf == "raft" {
				txn = s.engine.Raft.NewTransaction(true)
			} else {
				txn = s.engine.Kv.NewTransaction(true)
			}
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := modify.Data.(storage.Delete)
			var txn *badger.Txn
			if delete.Cf == "raft" {
				txn = s.engine.Raft.NewTransaction(true)
			} else {
				txn = s.engine.Kv.NewTransaction(true)
			}
			err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
			if err != nil {
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type StandaloneReader struct {
	kvTxn *badger.Txn
	raftTxn *badger.Txn
}

func (s *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var txn *badger.Txn
	if cf == "raft" {
		txn = s.raftTxn
	} else {
		txn = s.kvTxn
	}
	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	var txn *badger.Txn
	if cf == "raft" {
		txn = s.raftTxn
	} else {
		txn = s.kvTxn
	}
	return engine_util.NewCFIterator(cf, txn)
}

func (s *StandaloneReader) Close() {
	s.kvTxn.Discard()
	s.raftTxn.Discard()
}
