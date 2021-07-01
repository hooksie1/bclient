package bclient

import (
        "fmt"
        "go.etcd.io/bbolt"
)

type boltWriter interface {
        write() *boltTxn
}

type boltReader interface {
        read() *boltTxn
}

type boltDeleter interface {
        delete() *boltTxn
}

type validator interface {
        validate() *boltTxn
}

// BoltClient holds a Bolt DB connection
type BoltClient struct {
        DB     *bbolt.DB
}

// boltTxn holds a transaction function and a return value from the transaction.
type boltTxn struct {
       // returnValue allows for a value to be returned from a function. Since the function is returned
       // and then run, the value(s) can be stored in returnValue so it can be accessed outside of db.View()
       returnValue interface{}
       txn func(tx *bbolt.Tx) error
}

// NewClient returns a new BoltClient
func NewClient() *BoltClient {
        return &BoltClient{}
}

// NewDB creates a new database or opens an existing database and attaches it to the client.
func (b *BoltClient) NewDB(name string) error {
        var err error
        b.DB, err = bbolt.Open(name, 0644, nil)
        if err != nil {
                return fmt.Errorf("erorr openeing database: %s", err)
        }

        return nil
}

// Write is the entrypoint to write either a KV, a slice of KVs, or a Bucket.
func (b BoltClient) Write(w boltWriter) error {

        btrx := w.write()

        if err := b.DB.Update(btrx.txn); err != nil {
                return err
        }

        return nil
}

// Read is the entrypoint to read a KV or slice of KVs.
func (b BoltClient) Read(r boltReader) error {
        btrx := r.read()

         if err :=  b.DB.View(btrx.txn); err != nil {
                return err
        }

        return nil
}

// ReadAll is the entrypoint to read all KVs from a bucket.
func (b BoltClient) ReadAll(r boltReader) (KVs, error) {
        btrx := r.read()

        if err := b.DB.View(btrx.txn); err != nil {
                return nil, err
        }

        val, ok := btrx.returnValue.(KVs)
        if !ok {
                return nil, fmt.Errorf("error getting KVs")
        }

        return val, nil
}

// Delete is the entrypoint to delete a database resource.
func (b BoltClient) Delete(r boltDeleter) error {
        btrx := r.delete()

        if err := b.DB.Update(btrx.txn); err != nil {
                return err
        }

        return nil
}
