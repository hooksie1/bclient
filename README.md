# Bbolt Client

## Overview

I use bbolt in a few of my projects and repeating the same boilerplate to interact with the database gets tedious.
This is an attempt to make it a little less verbose to interact with bbolt.

## Usage

### Create a New Client/DB

```go
package 
import (
  bc "gitlab.com/hooksie1/bclient"
)
func main() {
 client := bc.NewClient()
 client.NewDB("mydb.db")
}
```

### Create a Bucket

```go
bucket := bc.NewBucket("test")
client.Write(bucket)
```

### Create a New KV

```go
kv := bc.NewKV().SetBucket(bucket.Name).
	SetKey("testkey").SetValue("testvalue")
client.Write(kv)
```

### Read KV
Reading a KV sets the value in the KV passed to read.

```go
kv := bc.NewKV().SetBucket(bucket.Name).
	SetKey("testkey")
client.Read(kv)
fmt.Println(kv.Value)
```

### Write a Slice of KVs
```go
kv1 := bc.NewKV().SetBucket(bucket.Name).
	SetKey("test").SetValue("test")
kv2 := bc.NewKV().SetBucket(bucket.Name).
	SetKey("somekey").SetValue("somevalue")

kvs := bc.KVs {
	kv1,
	kv2,
}

client.Write(kvs)
```

### Read All KVs from a Bucket

Returns a slice of KVs from the specified bucket.
```go
bucket := bc.NewBucket("testing")
kvs, err := client.ReadAll(bucket)
if err != nil {
	log.Println(err)
}

for _, v := range kvs {
	fmt.Printf("bucket: %s, key: %s, value: %s", v.Bucket, v.Key, v.Value)
}
```

### Delete a Bucket/KV/KVs

Use the same process for each type.

```go
bucket := bc.Newbucket("test")
client.Delete(bucket)
```

## More Advanced Usage

Since the client just embeds a `*bbolt.DB `you can access the `View` and `Update` methods directly.

```go
package
import (
	bc "gitlab.com/hooksie1/bclient"
)
func main() {
	client := bc.NewClient()
	client.NewDB("mydb.db")
	client.DB.View()
}   
```
