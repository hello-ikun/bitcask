package ikundb

// 固定的数据信息
const (
	minDataLength   = 12
	maxKeyValLength = (2 << 16) - 1
	nonBatch        = 0
)

type IndexType = byte

const (
	SyncMap IndexType = iota
	Btree
	OrderMap
	Art
	DeFault
)

// 配置信息
type Config struct {
	MapIndexType  IndexType
	DbPath        string
	MergeDbPath   string
	MaxDataLength int64
	*DbConfig
}

// db配置信息
type DbConfig struct {
	DbPrefix   string
	DbFidWidth int
	DbSuffix   string
}

// 默认配置信息
var DeFaultConfig = Config{
	MapIndexType:  DeFault,
	DbPath:        "./path/to/db",
	MergeDbPath:   "./path/to/merge_db",
	MaxDataLength: 1024,
	DbConfig: &DbConfig{
		DbPrefix:   "db_",
		DbFidWidth: 6,
		DbSuffix:   ".dat",
	},
}
