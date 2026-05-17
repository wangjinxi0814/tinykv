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
	reader, err := server.storage.Reader(req.GetContext())
	resp := new(kvrpcpb.RawGetResponse)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	resp.Value = value
	resp.NotFound = value == nil

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := new(kvrpcpb.RawPutResponse)
	err := server.storage.Write(req.GetContext(), []storage.Modify{{
		Data: storage.Put{
			Cf:    req.GetCf(),
			Key:   req.GetKey(),
			Value: req.GetValue(),
		},
	}})
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := new(kvrpcpb.RawDeleteResponse)
	err := server.storage.Write(req.GetContext(), []storage.Modify{{
		Data: storage.Delete{
			Cf:  req.GetCf(),
			Key: req.GetKey(),
		},
	}})
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Hint: Consider using reader.IterCF
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	for iter.Seek(req.GetStartKey()); iter.Valid() && uint32(len(resp.Kvs)) < req.GetLimit(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, nil
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: value,
		})
	}
	return resp, nil
}
