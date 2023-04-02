package bclient

import (
	"fmt"

	"go.etcd.io/bbolt"
)

// Bucket holds the name of a bucket in a BoltDB database.
type Bucket struct {
	Name   string             `json:"name,omitempty"`
	Nested map[string]*Bucket `json:"children,omitempty"`
	Parent *Bucket
}

// NewBucket returns a Bucket with the specified name.
func NewBucket(name string) *Bucket {
	return &Bucket{
		Name:   name,
		Nested: make(map[string]*Bucket),
	}
}

func (b *Bucket) SetNestedBucket(c *Bucket) {
	b.Nested[c.Name] = c
	c.Parent = b
}

// write writes a bucket to the database.
func (b Bucket) write() *boltTxn {
	return createIfNotExists(&b)
}

// validate validates whether a bucket exists or not.
func (b Bucket) exists() *boltTxn {
	return exists(b.Name)
}

// delete deletes the bucket.
func (b Bucket) delete() *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket([]byte(b.Name)); err != nil {
			return fmt.Errorf("error deleting bucket")
		}

		return nil
	}

	return &btxn
}

// read fetches all KV pairs in a bucket and returns the values in
// the return value of a boltTxn.
func (b Bucket) read() *boltTxn {
	var kvs KVs
	var btxn boltTxn
	btxn.txn = func(tx *bbolt.Tx) error {
		bucket := getBucket(tx, &b)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		cursor := bucket.Cursor()

		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			kv := &KV{
				Key:   string(k),
				Value: string(v),
			}
			kvs = append(kvs, kv)
		}
		// since the function is returned and then run, the value needs to be set in the returnValue
		// field of the boltTxn struct. This way the value(s) can be persisted after a db.View()
		btxn.returnValue = kvs
		return nil
	}

	return &btxn
}

// createifNotExists creates a new bucket in the database with the specified name
func createIfNotExists(b *Bucket) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		return createNestedBuckets(tx, b, nil)
	}

	return &btxn

}

func createNestedBuckets(tx *bbolt.Tx, bucket *Bucket, bbucket *bbolt.Bucket) error {
	var nested *bbolt.Bucket
	var err error
	if bbucket == nil {
		nested, err = tx.CreateBucketIfNotExists([]byte(bucket.Name))
	} else {
		nested, err = bbucket.CreateBucketIfNotExists([]byte(bucket.Name))
	}
	if err != nil {
		return err
	}

	for _, v := range bucket.Nested {
		if v.Nested != nil {
			createNestedBuckets(tx, v, nested)
		}
	}

	return nil
}

// exists returns false if the bucket does not exist
func exists(name string) *boltTxn {
	var btxn boltTxn
	btxn.returnValue = false

	btxn.txn = func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b != nil {
			btxn.returnValue = true
		}

		return nil
	}

	return &btxn
}

// getBucket takes a bbolt Transaction and a bucket. It traverses up the links of buckets to find the top level parent. Then
// traverses down the buckets to get the actual bucket. This is needed since the nested buckets are called by chaining together
// instead of just calling the bucket name directly.
func getBucket(tx *bbolt.Tx, b *Bucket) *bbolt.Bucket {
	buckets := []*Bucket{}
	head := b

	for head.Parent != nil {
		buckets = append(buckets, head)
		head = head.Parent
	}

	bucket := tx.Bucket([]byte(head.Name))

	for i := len(buckets) - 1; i >= 0; i-- {
		tmp := bucket.Bucket([]byte(buckets[i].Name))
		if tmp != nil {
			bucket = tmp
		}
	}

	return bucket
}
