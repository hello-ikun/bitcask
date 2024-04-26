package ikundb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
)

// IkunDb 数据库引擎
type IkunDb struct {
	index        mapIndex //kvMap
	olderFiles   map[uint64]*fileControl
	activeFile   *fileControl
	fid          uint64
	initialFlag  bool
	batchNum     uint32
	rw           *sync.RWMutex
	startMergeId uint64
	mergeId      uint64
}

var (
	maxDataLength int64 = 512
	dbPrefix            = "ikunDb"
	dbFidWidth          = 5
	dbSuffix            = ".ikun"
	dbPath              = "./ikunDbFile"
	mergeFilePath       = "./ikunMergeDbFile"
)

var (
	ErrKeyOrValLength = errors.New("error key or value length illgeal")
	ErrKeyNotFound    = errors.New("key not found")
	ErrFileFailed     = errors.New("文件存在丢失")
	ErrFile           = errors.New("文件丢失")
)

func DeFaultClient() *IkunDb {
	return DbClient(DeFaultConfig)
}
func DbClient(config Config) *IkunDb {
	if err := checkConfig(config); err != nil {
		panic(err)
	}

	ikun := &IkunDb{
		olderFiles: make(map[uint64]*fileControl),
		rw:         &sync.RWMutex{},
	}
	setDbConfig(config)
	ikun.index = indexers[config.MapIndexType]

	if err := ikun.init(); err != nil {
		panic(err)
	}
	return ikun
}
func checkConfig(config Config) error {
	if _, ok := indexers[config.MapIndexType]; !ok {
		config.MapIndexType = DeFault
	}
	return nil
}
func setDbConfig(dbConfig Config) {
	if dbConfig.MaxDataLength > maxDataLength {
		dbConfig.MaxDataLength = maxDataLength
	}
	if (len(dbConfig.DbPath)) != 0 {
		dbPath = dbConfig.DbPath
	}
	if len(dbConfig.MergeDbPath) != 0 {
		mergeFilePath = dbConfig.MergeDbPath
	}
	if dbConfig.DbConfig == nil {
		return
	}
	if len(dbConfig.DbPrefix) != 0 {
		dbPrefix = dbConfig.DbPrefix
	}
	if dbConfig.DbFidWidth > dbFidWidth {
		dbFidWidth = dbConfig.DbFidWidth
	}
	if len(dbConfig.DbSuffix) != 0 {
		dbSuffix = dbConfig.DbSuffix
	}
}
func NewDbClient() *IkunDb {
	ikun := &IkunDb{
		olderFiles: make(map[uint64]*fileControl),
		rw:         &sync.RWMutex{},
	}
	ikun.index = indexers[OrderMap]
	if err := ikun.init(); err != nil {
		panic(err)
	}
	return ikun
}
func (i *IkunDb) init() error {
	err := i.checkInitialFlag()
	if err != nil {
		panic(err)
	}
	if err := i.readDbFiles(); err != nil {
		panic(err)
	}
	if i.activeFile == nil {
		i.generateActiveFile()
	}
	return nil
}
func getFileName(fid uint64) string {
	return fmt.Sprintf("%s_%0*d%s", dbPrefix, dbFidWidth, fid, dbSuffix)
}
func (i *IkunDb) generateActiveFile() {
	if i.activeFile != nil {
		i.olderFiles[i.activeFile.fid] = i.activeFile
	}
	i.generateNewFid()
	i.activeFile = newFileControl(i.fid)
}
func (i *IkunDb) generateNewFid() {
	i.fid++
}
func (i *IkunDb) checkInitialFlag() error {
	var initialFlag = true
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		entries, err := os.ReadDir(dbPath)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			initialFlag = false
		}
	}
	i.initialFlag = initialFlag
	if i.initialFlag {
		if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
			return err
		}
		return nil
	}
	return nil
}

type tx struct {
	key []byte
	po  *pos
}

func (i *IkunDb) readDbFiles() error {
	if i.initialFlag {
		return nil
	}
	var st uint64
	// 读取merge文件
	da0, err := read(mergeFilePath)
	if err == nil {
		st = da0.dbStart

		// for fid := da0.start; fid <= da0.end; fid++ {

		entris, err := os.ReadDir(mergeFilePath)
		if err != nil {
			return err
		}

		for _, dir := range entris {
			if dir.Name() == mergeInfo {
				continue
			}
			fid, err := parseFid(dir.Name())
			if err != nil {
				return err
			}
			temp := newMergeControl(fid)
			if fid < da0.start {
				if err := temp.clear(); err != nil {
					return err
				}
				continue
			}

			var offset uint64
			for {
				da, length, err := temp.read(offset)
				if err != nil && err != io.EOF {
					return nil
				}
				if err == io.EOF {
					break
				}
				if len(da.value) != 0 { //正常数据插入即可
					i.index.set(da.key, &pos{fid: fid, offset: offset, length: length})
				} else {
					i.index.del(da.key) //删除数据删除即可
				}
				offset += length
			}
			temp.w.offset = int64(offset)
			i.olderFiles[fid] = temp
			i.mergeId = max(i.mergeId, fid)
		}
	}
	// 读取常规文件
	var minFid, maxFid uint64
	minFid = 1
	var curBatchNum uint32
	entris, err := os.ReadDir(dbPath)
	if err != nil {
		return err
	}
	txs := make(map[uint32][]*tx)
	txFinish := []uint32{}
	for _, dir := range entris {
		fid, err := parseFid(dir.Name())
		if err != nil {
			return err
		}
		if fid < st {
			continue
		}
		temp := newFileControl(fid)
		var offset uint64
		for {
			da, length, err := temp.read(offset)
			if err != nil && err != io.EOF {
				return nil
			}
			if err == io.EOF {
				break
			}
			if da.txCode == nonBatch {
				if len(da.value) != 0 { //正常数据插入即可
					i.index.set(da.key, &pos{fid: fid, offset: offset, length: length})
				} else {
					i.index.del(da.key) //删除数据删除即可
				}
			} else {
				if len(da.value) == 0 {
					txFinish = append(txFinish, da.txCode)
				} else {
					txs[da.txCode] = append(txs[da.txCode], &tx{key: da.key, po: &pos{fid: fid, offset: offset, length: length}})
				}
			}

			offset += length
		}
		temp.w.offset = int64(offset)
		i.olderFiles[fid] = temp
		minFid = min(fid, minFid)
		maxFid = max(fid, maxFid)
	}
	// 对于batch中的数据进行操作
	for _, batchNum := range txFinish {
		for _, v := range txs[batchNum] {
			i.index.set(v.key, v.po)
		}
	}
	// fmt.Println(txFinish, txs)
	if maxFid-minFid+1 != uint64(len(entris)) {
		return ErrFileFailed
	}
	i.activeFile = i.olderFiles[maxFid]
	i.fid = maxFid
	i.batchNum = curBatchNum
	return nil
}

// 根据fileName解析fid
func parseFid(filename string) (uint64, error) {
	length := len(dbPrefix) + 1
	end := length + dbFidWidth
	fid := filename[length:end]
	ans, err := strconv.Atoi(fid)
	return uint64(ans), err
}
func (i *IkunDb) getValue(p *pos) ([]byte, error) {
	var temp *fileControl
	if i.activeFile.fid == p.fid {
		temp = i.activeFile
	} else {
		temp = i.olderFiles[p.fid]
	}
	if temp == nil {
		return nil, ErrFile
	}
	da, _, err := readData(temp.fi, p.offset)
	if err != nil {
		return nil, err
	}
	if len(da.value) == 0 {
		return nil, ErrKeyNotFound
	}
	return da.value, nil
}

// 如果标记是删除呢? 如何标记是一个实物呢
// 很简单的
// 删除 我们只需要对于value加一个标记 + 比如在末尾添加一个flag
// 事物 我们也可以在key的末尾添加一个标记 ok解决了
func (i *IkunDb) Set(key, value []byte) error {
	if err1, err2 := checkKeyLength(key), checkKeyLength(value); err1 != nil || err2 != nil {
		return err1
	}
	i.rw.Lock()
	defer i.rw.Unlock()

	da := &data{
		txCode: nonBatch,
		key:    key,
		value:  value,
	}
	return i.set(da)
}

// 根据传入的data来设置
func (i *IkunDb) set(da *data) error {
	if i.activeFile.checkOverflow() {
		i.generateActiveFile()
	}
	pos, err := i.activeFile.write(da)
	if len(da.value) == 0 {
		return err
	}
	i.index.set(da.key, pos)
	return err
}

// 获取数据
func (i *IkunDb) Get(key []byte) ([]byte, error) {
	if err := checkKeyLength(key); err != nil {
		return nil, err
	}

	i.rw.RLock()
	defer i.rw.RUnlock()

	po, _ := i.index.get(key)
	if po == nil {
		i.index.show()
		return nil, ErrKeyNotFound
	}
	return i.getValue(po)
}

// 删除数据
func (i *IkunDb) Del(key []byte) error {
	if err := checkKeyLength(key); err != nil {
		return err
	}
	i.rw.Lock()
	defer i.rw.Unlock()
	// 检查是否存在这个key 如果不存在就不删除了
	if po, _ := i.index.get(key); po == nil {
		return ErrKeyNotFound
	}
	if i.activeFile.checkOverflow() {
		i.generateActiveFile()
	}
	da := &data{
		txCode: nonBatch,
		key:    key,
		value:  nil,
	}
	_, err := i.activeFile.write(da)
	i.index.del(key)
	return err
}

// 检查key的长度
func checkKeyLength(key []byte) error {
	if length := len(key); length >= maxKeyValLength || length <= 0 {
		return ErrKeyOrValLength
	}
	return nil
}

type Batch struct {
	ikun     *IkunDb
	batchMap sync.Map
}

func (i *IkunDb) NewBatch() *Batch {
	return &Batch{
		ikun:     i,
		batchMap: sync.Map{},
	}
}
func (b *Batch) Put(key, value []byte) error {
	if err1, err2 := checkKeyLength(key), checkKeyLength(value); err1 != nil || err2 != nil {
		return err1
	}
	b.batchMap.Store(string(key), value)
	return nil
}

func (b *Batch) Del(key []byte) error {
	if err := checkKeyLength(key); err != nil {
		return err
	}
	// 检查键是否存在
	if _, ok := b.batchMap.Load(string(key)); ok {
		b.batchMap.Delete(string(key))
	} else {
		if po, _ := b.ikun.index.get(key); po != nil {
			b.batchMap.Store(string(key), nil)
		} else {
			return ErrKeyNotFound
		}
	}
	return nil
}
func (b *Batch) Comment() error {
	b.ikun.batchNum++
	var err error
	b.batchMap.Range(func(key, value interface{}) bool {
		da := &data{
			txCode: b.ikun.batchNum,
			key:    []byte(key.(string)),
			value:  value.([]byte),
		}

		if err := b.ikun.set(da); err != nil {
			return false
		}
		return true
	})
	// 完成的标志
	buf := make([]byte, 4)
	bu := writeBuf(buf)
	bu.uint32(b.ikun.batchNum)
	fmt.Println(buf)
	da := &data{
		txCode: b.ikun.batchNum,
		key:    nil,
		value:  nil,
	}

	if err := b.ikun.set(da); err != nil {
		return err
	}
	b.batchMap = sync.Map{}
	return err
}

type merge struct {
	mergeNum   uint64
	activeFile *fileControl
}

func newMerge(mergeId uint64) *merge {
	m := &merge{mergeNum: mergeId}
	m.generateActiveFile()
	return m
}
func (i *IkunDb) Merge() error {
	i.startMergeId = i.mergeId
	i.generateActiveFile()
	dbStart := i.activeFile.fid
	m := newMerge(i.mergeId)
	start := m.mergeNum
	i.index.iter(func(key []byte, value *pos) bool {
		val, err := i.getValue(value)
		if err != nil {
			return false
		}

		err = m.mergeSet(&data{txCode: nonBatch, key: key, value: val})
		return err == nil
	})

	end := m.activeFile.fid

	da := &mergeData{start: start, end: end, dbStart: dbStart}
	return write(mergeFilePath, da)
}
func (m *merge) mergeSet(da *data) error {
	if m.activeFile == nil || m.activeFile.checkOverflow() {
		m.generateActiveFile()
	}
	_, err := m.activeFile.write(da)
	if len(da.value) == 0 {
		return err
	}
	return err
}
func (m *merge) generateActiveFile() {
	if m.activeFile != nil {
		m.activeFile = nil
	}
	m.generateNewFid()
	m.activeFile = newMergeControl(m.mergeNum)
}
func (m *merge) generateNewFid() {
	m.mergeNum++
}
func (i *IkunDb) Close() error {
	if i.activeFile != nil {
		_ = i.activeFile.close()
	}
	for _, v := range i.olderFiles {
		_ = v.close()
	}
	_ = i.index.close()
	i.rw = nil
	return nil
}
