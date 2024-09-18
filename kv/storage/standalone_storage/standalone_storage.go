package standalone_storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

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
	config *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	KvPath := filepath.Join(dbPath, "standalone_kv")

	os.MkdirAll(KvPath, os.ModePerm)

	Kv := engine_util.CreateDB(KvPath, false)
	engines := engine_util.NewEngines(Kv, nil, KvPath, "")

	return &StandAloneStorage{
		config: conf,
		engine: engines,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	result_reader := NewStandAloneReader(txn)
	return result_reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).'
	tmpwbatch := new(engine_util.WriteBatch)
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			tmpwbatch.SetCF(b.Cf(), b.Key(), b.Value())
		case storage.Delete:
			tmpwbatch.DeleteCF(b.Cf(), b.Key())
		}
	}
	err := tmpwbatch.WriteToDB(s.engine.Kv)
	return err
}

func (s *StandAloneStorage) Show_cf_all(cf string) {
	txn := s.engine.Kv.NewTransaction(false)
	elem_iterator := engine_util.NewCFIterator(cf, txn)
	for elem_iterator.Seek([]byte("")); elem_iterator.Valid(); elem_iterator.Next() {
		elem_key := elem_iterator.Item().Key()
		elem_val, err := elem_iterator.Item().Value()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(elem_key), " ", string(elem_val))
	}
}

type StandAloneReader struct {
	// Kv *badger.DB
	txn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.txn)
	iter.Rewind()
	return iter
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
