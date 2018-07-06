// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package etcdsimplekv

import (
	"bytes"
	"context"
	"time"

	"github.com/coreos/etcd/client"
	errgo "gopkg.in/errgo.v1"
	retry "gopkg.in/retry.v1"

	"github.com/juju/simplekv"
)

type kvStore struct {
	cl client.Client
}

func NewStore(cl client.Client) (simplekv.Store, error) {
	return &kvStore{cl}, nil
}

func (s *kvStore) Context(ctx context.Context) (_ context.Context, close func()) {
	return ctx, func() {}
}

func (s *kvStore) Get(ctx context.Context, key string) ([]byte, error) {
	api := client.NewKeysAPI(s.cl)
	resp, err := api.Get(ctx, key, nil)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return nil, simplekv.KeyNotFoundError(key)
		}
		return nil, errgo.Mask(err)
	}
	if len(resp.Node.Nodes) > 0 {
		return nil, errgo.Newf("not a single value: %q", key)
	}
	return []byte(resp.Node.Value), nil
}

func (s *kvStore) Set(ctx context.Context, key string, value []byte, expire time.Time) error {
	api := client.NewKeysAPI(s.cl)
	opts := client.SetOptions{}
	ttl := expire.Sub(time.Now())
	if ttl > 0 {
		opts.TTL = ttl
	}
	_, err := api.Set(ctx, key, string(value), &opts)
	return errgo.Mask(err)
}

func (s *kvStore) Update(ctx context.Context, key string, expire time.Time, getVal func(old []byte) ([]byte, error)) error {
	api := client.NewKeysAPI(s.cl)
	opts := client.SetOptions{}
	ttl := expire.Sub(time.Now())
	if ttl > 0 {
		opts.TTL = ttl
	}
	r := retry.StartWithCancel(updateStrategy, nil, ctx.Done())
	for r.Next() {
		respGet, err := api.Get(ctx, key, nil)
		if err != nil {
			if client.IsKeyNotFound(err) {
				newVal, err := getVal(nil)
				if err != nil {
					return errgo.Mask(err, errgo.Any)
				}
				newOpts := opts
				newOpts.PrevExist = client.PrevNoExist
				_, err = api.Set(ctx, key, string(newVal), &newOpts)
				if isConflict(err) {
					continue
				}
			}
			return errgo.Mask(err)
		} else if len(respGet.Node.Nodes) > 0 {
			return errgo.Newf("not a single value: %q", key)
		}

		newVal, err := getVal([]byte(respGet.Node.Value))
		if err != nil {
			return errgo.Mask(err, errgo.Any)
		}
		if bytes.Equal(newVal, []byte(respGet.Node.Value)) {
			return nil
		}

		updateOpts := opts
		updateOpts.PrevIndex = respGet.Index
		_, err = api.Set(ctx, key, string(newVal), &updateOpts)
		if err == nil {
			return nil
		}
		if !isConflict(err) {
			return errgo.Mask(err)
		}
		// keep trying..
	}
	if r.Stopped() {
		return errgo.Notef(ctx.Err(), "cannot update key")
	}
	return errgo.Newf("too many retry attempts trying to update key")
}

func isConflict(err error) bool {
	if err == nil {
		return false
	}
	if etcdErr, ok := err.(client.Error); ok {
		switch etcdErr.Code {
		case client.ErrorCodeKeyNotFound, client.ErrorCodeNodeExist,
			client.ErrorCodeTestFailed:
			return true
		}
	}
	return false
}

var updateStrategy = retry.Exponential{
	Initial:  time.Microsecond,
	Factor:   2,
	MaxDelay: 500 * time.Millisecond,
	Jitter:   true,
}
