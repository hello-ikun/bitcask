package bitcask_go

import (
	ikundb "github.com/hello-ikun/bitcask/ikunDb"
)

// Db 是对 ikundb.IkunDb 的包装
type Db struct {
	*ikundb.IkunDb
}

// NewDbClient 创建一个新的 Db 客户端
func NewDbClient() *Db {
	return &Db{ikundb.NewDbClient()}
}
func DbClient(config ikundb.Config) *Db {
	return &Db{ikundb.DbClient(config)}
}
func DefaultClient() *Db {
	return &Db{ikundb.DeFaultClient()}
}

// Put 向数据库中放置键值对
func (db *Db) Put(key, val []byte) error {
	return db.IkunDb.Set(key, val)
}

// Get 从数据库中获取键对应的值
func (db *Db) Get(key []byte) ([]byte, error) {
	return db.IkunDb.Get(key)
}

// Del 从数据库中删除键值对
func (db *Db) Del(key []byte) error {
	return db.IkunDb.Del(key)
}

// Merge 执行数据库的合并操作
func (db *Db) Merge() error {
	return db.IkunDb.Merge()
}

// Close 关闭数据库连接
func (db *Db) Close() error {
	return db.IkunDb.Close()
}

// Batch 是对 ikundb.Batch 的包装
type Batch struct {
	*ikundb.Batch
}

// Batch 返回一个新的批处理对象
func (db *Db) Batch() Batch {
	return Batch{db.NewBatch()}
}

// Put 将键值对添加到批处理中
func (b *Batch) Put(key, val []byte) error {
	return b.Batch.Put(key, val)
}

// Del 从批处理中删除键值对
func (b *Batch) Del(key []byte) error {
	return b.Batch.Del(key)
}

// Commit 提交批处理中的操作
func (b *Batch) Commit() error {
	return b.Batch.Comment() // 注意这里应为 Commit
}
