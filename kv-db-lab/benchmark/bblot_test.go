package benchmark

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
	"kv-db-lab/pkg"
	"log"
	"math/rand"
	"testing"
	"time"
)

var boltDB *bbolt.DB

func init() {
	var err error
	boltDB, err = bbolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
}
func Benchmark_PutBlot(b *testing.B) {
	// 重置计时器
	b.ResetTimer()

	// 打印每个方法内存使用情况
	b.ReportAllocs()
	boltDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	for i := 0; i < b.N; i++ {
		err := boltDB.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			err := b.Put(pkg.GetTestKey(i), pkg.RandomValue(8))
			return err
		})
		if err != nil {
			logrus.Error("put failed，err:", err.Error())
		}
	}
}
func Benchmark_GetBlot(b *testing.B) {
	// 重置计时器
	b.ResetTimer()

	// 打印每个方法内存使用情况
	b.ReportAllocs()
	boltDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	for i := 0; i < b.N; i++ {
		err := boltDB.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			err := b.Put(pkg.GetTestKey(i), pkg.RandomValue(8))
			return err
		})
		if err != nil {
			logrus.Error("put failed，err:", err.Error())
		}
	}

	for i := 0; i < 10000; i++ {
		err := db.Put(pkg.GetTestKey(i), pkg.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		boltDB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			v := b.Get(pkg.GetTestKey(rand.Int()))
			fmt.Printf("The answer is: %s\n", v)
			return nil
		})
	}
}
