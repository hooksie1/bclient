package bclient

import (
	"fmt"

	"go.etcd.io/bbolt"
)

// Bucket holds the name of a bucket in a BoltDB database.
type Bucket struct {
	Name string `json:"name,omitempty"`
}

// NewBucket returns a Bucket with the specified name.
func NewBucket(name string) *Bucket {
	return &Bucket{
		Name: name,
	}
}

// write writes a bucket to the database.
func (b Bucket) write() *boltTxn {
	return createIfNotExists(b.Name)
}

// validate validates whether a bucket exists or not.
func (b Bucket) validate() *boltTxn {
	return validate(b.Name)
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
		bucket := tx.Bucket([]byte(b.Name))
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
func createIfNotExists(name string) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		return nil
	}

	return &btxn

}

// validate returns an BucketNotFound error if a bucket does not exist
func validate(name string) *boltTxn {
	var btxn boltTxn

	btxn.txn = func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return bbolt.ErrBucketNotFound
		}

		return nil
	}

	return &btxn
}
