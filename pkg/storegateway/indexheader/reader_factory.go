// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storegateway/threadpool"
)

var DefaultReaderFactory = ReaderFactoryFunc(NewBinaryReader)

type ReaderFactory interface {
	NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (Reader, error)
}

type ReaderFactoryFunc func(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (Reader, error)

func (f ReaderFactoryFunc) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (Reader, error) {
	return f(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, cfg)
}

type threadedReaderFactory struct {
	pool *threadpool.ThreadPool
}

func NewThreadedReaderFactory(pool *threadpool.ThreadPool) ReaderFactory {
	return &threadedReaderFactory{pool: pool}
}

func (f *threadedReaderFactory) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg BinaryReaderConfig) (Reader, error) {
	res, err := f.pool.Execute(func() (interface{}, error) {
		return NewBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, cfg)
	})

	if err != nil {
		return nil, err
	}

	return res.(Reader), nil
}
