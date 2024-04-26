package ikundb

import (
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	crcLength    = 4
	headerLength = 4 + 8
)

// 写入的数据是 crc32 keyLen valueLen key value
// 其中crc32是固定长度 使用uin32写入
// 注意 crc写入是需要等待 keyLen valueLen key value写入完毕 检验数据之后写入
// keyLen valueLen 也是固定长度 4位 使用uin32写入
type data struct {
	txCode uint32
	key    []byte
	value  []byte
}

// 需要讲写入的数据位置信息返回回来 到 pos 并且更新offset指针
type pos struct {
	fid    uint64
	offset uint64
	length uint64
}

var (
	ErrWriterFailed = errors.New("error writer failed")
	ErrCrcMismatch  = errors.New("error crc check failed")
)

// 数据写入
func (w *writer) writeData(info *data) (int, error) {
	keyLen, valLen := len(info.key), len(info.value)
	length := headerLength + keyLen + valLen + 4
	// 懒增加
	if w.offset+int64(length) > int64(maxDataLength) {
		w.last = true
	}
	buf := make([]byte, length)
	b := writeBuf(buf[:])

	b.uint32(uint32(len(info.key)))
	b.uint32(uint32(len(info.value)))
	b.uint32(info.txCode)

	b.write(info.key)
	b.write(info.value)
	crcSum := crc32.ChecksumIEEE(buf[:length-4])
	b.uint32(crcSum)
	if _, err := w.w.Write(buf); err != nil {
		return 0, ErrWriterFailed
	}
	return length, nil
}

type writer struct {
	w      io.Writer
	offset int64
	last   bool
}

// newWriter 创建一个新的 writer 对象
func newWriter(w io.Writer) *writer {
	return &writer{
		w: w,
	}
}

func readData(r io.ReaderAt, offset uint64) (*data, uint64, error) {
	buf := make([]byte, headerLength)
	if _, err := r.ReadAt(buf, int64(offset)); err != nil {
		return nil, 0, err
	}
	b := readBuf(buf)
	crc32Sum := crc32.ChecksumIEEE(buf)
	keySize := b.uint32()
	valueSize := b.uint32()
	txCode := b.uint32()

	totalSize := int(keySize) + int(valueSize) + crcLength
	buf = make([]byte, totalSize)
	if _, err := r.ReadAt(buf, int64(offset)+int64(headerLength)); err != nil {
		return nil, 0, err
	}
	key := buf[:keySize]
	val := buf[keySize : keySize+valueSize]
	crc32Sum = crc32.Update(crc32Sum, crc32.IEEETable, buf[:keySize+valueSize])
	b = buf[keySize+valueSize:]
	if crcSum := b.uint32(); crcSum != crc32Sum {
		return nil, 0, ErrCrcMismatch
	}
	return &data{txCode, key, val}, uint64(headerLength + totalSize), nil
}

// write 写入数据
func (w *writer) write(info *data) (*pos, error) {
	n, err := w.writeData(info)
	offset := w.offset
	if err == nil {
		w.offset += int64(n)
	}
	return &pos{offset: uint64(offset), length: uint64(n)}, err
}

// Offset 返回当前写入位置的偏移量
func (w *writer) Offset() int64 {
	return w.offset
}

// fileControl 直接调用的接口
type fileControl struct {
	fid uint64
	fi  *os.File
	w   *writer
}

// newFileControl 产生的新的实例
func newFileControl(fid uint64) *fileControl {
	return newDataFileControl(dbPath, fid)
}
func newMergeControl(fid uint64) *fileControl {
	return newDataFileControl(mergeFilePath, fid)
}
func newDataFileControl(filePath string, fid uint64) *fileControl {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		if err := os.Mkdir(filePath, os.ModePerm); err != nil {
			return nil
		}
		return nil
	}
	filename := filepath.Join(filePath, getFileName(fid))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil
	}
	return &fileControl{fi: file, w: newWriter(file), fid: fid}
}

// write 数据写入
func (fc *fileControl) write(info *data) (*pos, error) {
	po, err := fc.w.write(info)
	po.fid = fc.fid
	return po, err
}

// read 数据读取
func (fc *fileControl) read(offset uint64) (*data, uint64, error) {
	return readData(fc.fi, offset)
}

// 关闭文件
func (fc *fileControl) close() error {
	return fc.fi.Close()
}

// 同步文件
func (fc *fileControl) sync() error {
	return fc.fi.Sync()
}

// checkOverflow 判断是否已经写满了数据
func (fc *fileControl) checkOverflow() bool {
	return fc.w.last
}

// 删除文件
func (fc *fileControl) clear() error {
	if fc.fi != nil {
		fc.fi.Close()
	}
	return os.Remove(fc.fi.Name())
}

// 实现对于merge后的数据进行读取检查
const (
	mergeInfo = "db.config"
)

type mergeData struct {
	start   uint64
	end     uint64
	dbStart uint64
}

func write(path string, da *mergeData) error {
	buf := make([]byte, 8*3)
	b := writeBuf(buf)
	b.uint64(da.start)
	b.uint64(da.end)
	b.uint64(da.dbStart)
	return os.WriteFile(filepath.Join(path, mergeInfo), buf, os.ModePerm)
}
func read(path string) (*mergeData, error) {
	data, err := os.ReadFile(filepath.Join(path, mergeInfo))
	if err != nil {
		return nil, err
	}

	buf := readBuf(data)
	return &mergeData{
		start:   buf.uint64(),
		end:     buf.uint64(),
		dbStart: buf.uint64(),
	}, nil
}
