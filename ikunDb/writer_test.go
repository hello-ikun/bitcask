package ikundb

import (
	"fmt"
	"os"
	"testing"
)

func TestWriter(t *testing.T) {
	f, err := os.Create("1.txt")
	t.Log(err)
	w := newWriter(f)
	// crc32 980326536
	pos, err := w.write(&data{key: []byte("xia"), value: []byte("sang")})
	t.Log(pos, err)
	// f, err = os.Open("1.txt")
	data, length, err := readData(f, 0)
	t.Log(data, length, err)
}
func TestWriter0(t *testing.T) {
	f, err := os.Create("1.txt")
	t.Log(err)
	w := newWriter(f)
	// crc32 980326536
	pos, err := w.write(&data{key: []byte("xia"), value: []byte("sang")})
	t.Log(pos, err)
	// f, err = os.Open("1.txt")
	data, length, err := readData(f, 0)
	t.Log(data, length, err)
}
func TestWriter1(t *testing.T) {
	po := &pos{2, 380, 38}
	f := newFileControl(po.fid)
	buf := make([]byte, 38)
	t.Log(f.fi.ReadAt(buf, 380))
	t.Log(buf)

	buf1 := make([]byte, 38)
	t.Log(f.fi.ReadAt(buf1, 388))
	t.Log(buf1)
	stat, err := os.Stat(f.fi.Name())
	t.Log(err)
	t.Log(stat.Size())
	t.Log(f.read(po.offset))
	t.Log(readData(f.fi, 380))
}
func TestIkun(t *testing.T) {
	ikun := NewDbClient()
	// key := []byte("xia")
	value := []byte("sang")
	ikun.generateActiveFile()
	for i := 0; i < 12; i++ {
		ikun.Set([]byte(fmt.Sprintf("test_key:%09d", i)), value)
	}
	ikun.index.show()
}
func TestFid(t *testing.T) {
	ans := getFileName(12)
	t.Log(ans)
	t.Log(parseFid(ans))
}
func TestRead(t *testing.T) {
	ikun := NewDbClient()
	ikun.generateActiveFile()
	err := ikun.readDbFiles()
	t.Log(err)
	value := []byte("sang")
	for i := 0; i < 20; i++ {
		ikun.Set([]byte(fmt.Sprintf("test_key:%09d", i)), value)
	}
	ikun.index.show()

	for i := 0; i < 19; i++ {
		// get, err := ikun.Get([]byte(fmt.Sprintf("test_key:%09d", i)))
		// t.Log(get, err)
		err = ikun.Del([]byte(fmt.Sprintf("test_key:%09d", i)))
		t.Log(err)
	}
	for i := 0; i < 20; i++ {
		get, err := ikun.Get([]byte(fmt.Sprintf("test_key:%09d", i)))
		t.Log(get, err)

	}
}
func TestRead1(t *testing.T) {
	ikun := NewDbClient()
	ikun.generateActiveFile()
	err := ikun.readDbFiles()
	t.Log(err)

	for i := 0; i < 20; i++ {
		get, err := ikun.Get([]byte(fmt.Sprintf("test_key:%09d", i)))
		t.Log(get, err)

	}
}
func TestRead2(t *testing.T) {
	ikun := NewDbClient()
	// batch := ikun.NewBatch()
	// value := []byte("sang")
	// for i := 0; i < 100; i++ {
	// 	batch.Put([]byte(fmt.Sprintf("test_key:%09d", i)), value)
	// }
	// ikun.index.show()

	// batch.Comment()
	// ikun.index.show()
	// for i := 20; i < 100; i++ {
	// 	ikun.Del([]byte(fmt.Sprintf("test_key:%09d", i)))
	// }
	// t.Log(ikun.Merge())
	for i := 0; i < 30; i++ {
		get, err := ikun.Get([]byte(fmt.Sprintf("test_key:%09d", i)))
		t.Log(get, err)
	}
	fmt.Println(ikun.olderFiles, ikun.activeFile)
}
func TestRead0(t *testing.T) {
	t.Log(read(mergeFilePath))
}
