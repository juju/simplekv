// Copyright 2018 Canonical Ltd.
// Licensed under the LGPL, see LICENCE file for details.

package mgosimplekv

import (
	"bytes"
	"context"
	"time"

	mgo "github.com/juju/mgo/v2"
	"github.com/juju/mgo/v2/bson"
	errgo "gopkg.in/errgo.v1"
	retry "gopkg.in/retry.v1"

	"github.com/juju/simplekv"
)

type sessionKey struct{}

// kvStore implements simplekv.Store.
type kvStore struct {
	coll *mgo.Collection
}

// NewStore returns a new Store implementation that uses
// the given mongo collection for storage.
func NewStore(coll *mgo.Collection) (simplekv.Store, error) {
	if err := coll.EnsureIndex(mgo.Index{
		Key:         []string{"expire"},
		ExpireAfter: time.Second,
	}); err != nil {
		return nil, errgo.Mask(err)
	}
	return &kvStore{
		coll: coll,
	}, nil
}

// Context implements simplekv.Context by copying the kvStore's underlying
// session if one isn't already present in the context.
func (s *kvStore) Context(ctx context.Context) (_ context.Context, close func()) {
	if session, _ := ctx.Value(sessionKey{}).(*mgo.Session); session != nil {
		return ctx, func() {}
	}
	// TODO provide some way for the caller to associate their own
	// session with the context so that they can implement session
	// pooling if desired?
	session := s.coll.Database.Session.Copy()
	return ContextWithSession(ctx, session), session.Close
}

// session returns a *mgo.Session for use in subsequent queries. The returned
// session must be closed once finished with.
func (s *kvStore) session(ctx context.Context) *mgo.Session {
	if s, _ := ctx.Value(sessionKey{}).(*mgo.Session); s != nil {
		return s.Clone()
	}
	return s.coll.Database.Session.Copy()
}

// c returns the store's collection associated with any session found in the
// context. The collection's underlying session must be closed when the
// query is complete.
func (s *kvStore) c(ctx context.Context) *mgo.Collection {
	return s.coll.With(s.session(ctx))
}

type kvDoc struct {
	Key    string    `bson:"_id"`
	Value  []byte    `bson:"value'`
	Expire time.Time `bson:",omitempty"`
}

// Get implements simplekv.Store.Get by retrieving the document with
// the given key from the store's collection.
func (s *kvStore) Get(ctx context.Context, key string) ([]byte, error) {
	coll := s.c(ctx)
	defer coll.Database.Session.Close()

	var doc kvDoc
	if err := coll.FindId(key).One(&doc); err != nil {
		if errgo.Cause(err) == mgo.ErrNotFound {
			return nil, simplekv.KeyNotFoundError(key)
		}
		return nil, errgo.Mask(err)
	}
	return doc.Value, nil
}

// Set implements simplekv.Store.Set by upserting the document with
// the given key, value and expire time into the store's collection.
func (s *kvStore) Set(ctx context.Context, key string, value []byte, expire time.Time) error {
	coll := s.c(ctx)
	defer coll.Database.Session.Close()

	_, err := coll.UpsertId(key, bson.D{{
		"$set", bson.D{{
			"value", value,
		}, {
			"expire", expire,
		}},
	}})
	return errgo.Mask(err)
}

var updateStrategy = retry.Exponential{
	Initial:  time.Microsecond,
	Factor:   2,
	MaxDelay: 500 * time.Millisecond,
	Jitter:   true,
}

// Update implements simplekv.Store.Update.
func (s *kvStore) Update(ctx context.Context, key string, expire time.Time, getVal func(old []byte) ([]byte, error)) error {
	coll := s.c(ctx)
	defer coll.Database.Session.Close()

	r := retry.StartWithCancel(updateStrategy, nil, ctx.Done())
	for r.Next() {
		var doc kvDoc
		if err := coll.Find(bson.D{{"_id", key}}).One(&doc); err != nil {
			if errgo.Cause(err) != mgo.ErrNotFound {
				return errgo.Mask(err)
			}
			newVal, err := getVal(nil)
			if err != nil {
				return errgo.Mask(err, errgo.Any)
			}
			err = coll.Insert(kvDoc{
				Key:    key,
				Value:  newVal,
				Expire: expire,
			})
			if err == nil {
				return nil
			}
			if !mgo.IsDup(err) {
				return errgo.Mask(err)
			}
			// A new document has been inserted after we did the FindId and before Insert,
			// so try again.
			continue
		}
		newVal, err := getVal(doc.Value)
		if err != nil {
			return errgo.Mask(err, errgo.Any)
		}
		if bytes.Equal(newVal, doc.Value) {
			return nil
		}
		err = coll.Update(bson.D{{
			"_id", key,
		}, {
			"value", doc.Value,
		}}, bson.D{{
			"$set", bson.D{{
				"value", newVal,
			}, {
				"expire", expire,
			}},
		}})
		if err == nil {
			return nil
		}
		if err != mgo.ErrNotFound {
			return errgo.Mask(err)
		}
		// The document has been removed or updated since we retrieved it,
		// so try again.
	}
	if r.Stopped() {
		return errgo.Notef(ctx.Err(), "cannot update key")
	}
	return errgo.Newf("too many retry attempts trying to update key")
}

// ContextWithSession returns the given context associated with the given
// session. When the context is passed to one of the Store methods,
// the session will be used for database access.
func ContextWithSession(ctx context.Context, session *mgo.Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, session)
}
