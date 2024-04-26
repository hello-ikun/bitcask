package ikundb

import "encoding/binary"

type writeBuf []byte

func (b *writeBuf) uint32(v uint32) {
	binary.LittleEndian.PutUint32(*b, v)
	*b = (*b)[4:]
}

func (b *writeBuf) uint64(v uint64) {
	binary.LittleEndian.PutUint64(*b, v)
	*b = (*b)[8:]
}
func (b *writeBuf) write(data []byte) {
	copy(*b, data)
	*b = (*b)[len(data):]
}

// 参考go语言官方实现
type readBuf []byte

func (b *readBuf) uint32() uint32 {
	v := binary.LittleEndian.Uint32(*b)
	*b = (*b)[4:]
	return v
}

func (b *readBuf) uint64() uint64 {
	v := binary.LittleEndian.Uint64(*b)
	*b = (*b)[8:]
	return v
}
func (b *readBuf) read() []byte {
	remaining := *b
	*b = (*b)[len(*b):]
	return remaining
}
