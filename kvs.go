package bclient

import (
	"fmt"

	"go.etcd.io/bbolt"
)

type KV struct {
	Bucket string `json:"bucket,omitempty"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
}

type KVs []*KV

// NewKV returns a new empty KV.
func NewKV() *KV {
	return &KV{}
}

// SetKey sets the key for a KV.
func (kv *KV) SetKey(k string) *KV {
	kv.Key = k

	return kv
}

// SetBucket sets the bucket value for a KV.
func (kv *KV) SetBucket(b string) *KV {
	kv.Bucket = b

	return kv
}

// SetValue sets the value for a KV.
func (kv *KV) SetValue(v string) *KV {
	kv.Value = v

	return kv
}

// write creates a bolt transaction function.
func (kv KV) write() *boltTxn {
	return createKV(kv)
}

// write creates a bolt transaction function.
func (kvs KVs) write() *boltTxn {
	return createKVs(kvs)
}

// delete creates a bolt transaction function.
func (kv KV) delete() *boltTxn {
	return deleteKV(kv)
}

// delete creates a b olt transaction function
func (kvs KVs) delete() *boltTxn {
	return deleteKVs(kvs)
}

// read fetches the data from the KV bucket/key in the KV and sets the KV value.
func (kv *KV) read() *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(kv.Bucket))
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}

		kv.Value = string(bucket.Get([]byte(kv.Key)))

		return nil
	}

	return &btxn
}

// read fetches the values for the KV pairs in the KV slice.
func (kvs KVs) read() *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		for _, v := range kvs {
			bucket := tx.Bucket([]byte(v.Bucket))
			if bucket == nil {
				return bbolt.ErrBucketNotFound
			}
			v.Value = string(bucket.Get([]byte(v.Key)))

		}

		return nil
	}

	return &btxn
}

// createKV creates a new RW bolt transaction for the given KV.
func createKV(kv KV) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(kv.Bucket))
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}

		if err := bucket.Put([]byte(kv.Key), []byte(kv.Value)); err != nil {
			return fmt.Errorf("error creating kv pair")
		}

		return nil
	}

	return &btxn
}

// createKVs creates a new RW bolt transaction for the slice of KVs.
func createKVs(kvs KVs) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		for _, v := range kvs {
			bucket := tx.Bucket([]byte(v.Bucket))
			if bucket == nil {
				return bbolt.ErrBucketNotFound
			}
			if err := bucket.Put([]byte(v.Key), []byte(v.Value)); err != nil {
				return fmt.Errorf("error creating kv pair")
			}
		}
		return nil
	}

	return &btxn
}

// deleteKV deletes a KV pair in a bucket.
func deleteKV(kv KV) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(kv.Bucket))
		if bucket == nil {
			return bbolt.ErrBucketNotFound
		}
		if err := bucket.Delete([]byte(kv.Key)); err != nil {
			return fmt.Errorf("error deleting kv")
		}
		return nil
	}

	return &btxn
}

// deleteKVs deletes a set of KV pairs.
func deleteKVs(kvs KVs) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		for _, v := range kvs {
			bucket := tx.Bucket([]byte(v.Bucket))
			if bucket == nil {
				return bbolt.ErrBucketNotFound
			}
			if err := bucket.Delete([]byte(v.Key)); err != nil {
				return fmt.Errorf("error deleting key")
			}
		}
		return nil
	}

	return &btxn
}
