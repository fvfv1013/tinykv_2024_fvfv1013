package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	result := new(kvrpcpb.RawGetResponse)
	result.NotFound = true

	reader, err := server.storage.Reader(nil)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)

	if val == nil {
		return result, nil
	}
	if err != nil {
		result.Error = err.Error()
		return result, err
	}
	result.NotFound = false
	result.Value = val
	return result, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	result := new(kvrpcpb.RawPutResponse)

	batch := storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}
	packed_batch := storage.Modify{
		Data: batch,
	}

	err := server.storage.Write(nil, []storage.Modify{packed_batch})
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	return result, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	result := new(kvrpcpb.RawDeleteResponse)

	batch := storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}
	packed_batch := storage.Modify{
		Data: batch,
	}

	err := server.storage.Write(nil, []storage.Modify{packed_batch})
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	return result, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	result := new(kvrpcpb.RawScanResponse)
	kvs := []*kvrpcpb.KvPair{}

	reader, err := server.storage.Reader(nil)
	if err != nil {
		result.Error = err.Error()
		return result, err
	}

	itor := reader.IterCF(req.Cf)
	var cnt uint32 = 0
	for itor.Seek(req.StartKey); itor.Valid(); itor.Next() {
		val, err := itor.Item().Value()
		if err != nil {
			result.Error = err.Error()
			return result, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   itor.Item().Key(),
			Value: val,
		})
		cnt++

		if cnt >= req.Limit {
			break
		}
	}
	result.Kvs = kvs

	return result, nil
}
