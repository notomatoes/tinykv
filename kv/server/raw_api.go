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
	response := &kvrpcpb.RawGetResponse{NotFound: false}

	reader, _ := server.storage.Reader(req.Context)
	value, err := reader.GetCF(req.Cf, req.Key)

	if value == nil || err != nil {
		response.NotFound = true
	}
	response.Value = value

	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}

	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	batch := storage.Modify{Data: put}
	err := server.storage.Write(req.Context, []storage.Modify{batch})

	return response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}

	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	batch := storage.Modify{Data: del}
	err := server.storage.Write(req.Context, []storage.Modify{batch})

	return response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var kvPairs []*kvrpcpb.KvPair
	response := &kvrpcpb.RawScanResponse{}

	reader, _ := server.storage.Reader(req.Context)
	limit := req.Limit

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)

	for ; iter.Valid(); iter.Next() {
		key := iter.Item().Key()
		value, _ := iter.Item().Value()
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: key, Value: value})

		limit--
		if limit == 0 {
			break
		}
	}
	response.Kvs = kvPairs
	return response, nil
}
