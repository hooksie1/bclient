package main

import (
	"fmt"
	"log"

	"github.com/hooksie1/bclient"
)

func main() {
	client := bclient.NewClient()
	client.NewDB("test.db")

	// storing data
	artists := bclient.NewBucket("Artists")
	ts := bclient.NewBucket("Twisted Sister")
	christmasAlbum := bclient.NewBucket("A Twisted Christmas")
	songs := bclient.NewBucket("songs")

	artists.SetNestedBucket(ts)
	ts.SetNestedBucket(christmasAlbum)
	christmasAlbum.SetNestedBucket(songs)

	if err := client.Write(songs); err != nil {
		log.Fatal(err)
	}

	kvs := bclient.KVs{
		bclient.NewKV().SetBucket(songs).SetKey("Have Yourself A Merry Little Christmas").
			SetValue(`{"length": "4:48", "writers": ["Hugh Martin", "Ralph Blane"]}`),
		bclient.NewKV().SetBucket(songs).SetKey("Oh Come All Ye Faithful").
			SetValue(`{"length": "4:40", "writers": ["Traditional"]}`),
	}

	if err := client.Write(kvs); err != nil {
		log.Fatal(err)
	}

	// lookup all keys
	songList, err := client.ReadAll(songs)
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range songList {
		fmt.Println(v.Key)
		fmt.Println(v.Value)
	}

	// lookup single key
	single := bclient.NewKV().SetBucket(songs).SetKey("Oh Come All Ye Faithful")

	if err := client.Read(single); err != nil {
		log.Fatal(err)
	}

	fmt.Println(single.Key)
	fmt.Println(single.Value)
}
