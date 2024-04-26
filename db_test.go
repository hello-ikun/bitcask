package bitcask_go

import (
	"fmt"
	"testing"
)

func TestDb_Del(t *testing.T) {
	db := DefaultClient() //NewDbClient()
	for i := 0; i < 100; i++ {
		key, val := fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i)
		err := db.Put([]byte(key), []byte(val))
		if err != nil {
			t.Log(err)
		}
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		da, err := db.Get([]byte(key))
		t.Log(string(da), err)
	}
	db.Merge()
}
