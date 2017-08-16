// https://godoc.org/golang.org/x/exp/mmap

// Look here to figure out how to pass FDs over domain socket:
//   src/syscall/syscall_unix_test.go

// https://github.com/andrenth/go-fdpass/blob/master/fdpass.go
// https://github.com/ftrvxmtrx/fd/blob/master/fd.go
// https://github.com/mindreframer/golang-stuff/blob/master/github.com/youtube/vitess/go/umgmt/fdpass.go
// http://stackoverflow.com/questions/8362747/how-can-i-detect-whether-a-specific-page-is-mapped-in-memory

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"syscall"
	"unsafe"

	capnp "zombiezen.com/go/capnproto2"
)

const (
	// include/uapi/linux/memfd.h
	MFD_CLOEXEC       = 0x0001
	MFD_ALLOW_SEALING = 0x0002

	// arch/x86/syscalls/syscall_64.tbl
	SYS_MEMFD_CREATE = 319
)

var (
	pageSize              = syscall.Getpagesize()
	errSegmentOutOfBounds = errors.New("capnp: segment ID out of bounds")
)

// TODO:
// mmap in multiples of page size,
// keep track of last extent and, while possible, allocate subsequent segments
// from it.
// might need to keep track of mappings separately from segments.
type MmapArena struct {
	file *os.File
	off  int64
	segs map[capnp.SegmentID][]byte
	maps [][]byte
}

func NewMmapArena(file *os.File, off int) *MmapArena {
	return &MmapArena{
		file: file,
		off:  int64(off),
		segs: map[capnp.SegmentID][]byte{},
		maps: [][]byte{},
	}
}

func (self *MmapArena) NumSegments() int64 {
	return int64(len(self.segs))
}

func (self *MmapArena) Data(id capnp.SegmentID) ([]byte, error) {
	if int64(id) >= int64(len(self.segs)) {
		return nil, errSegmentOutOfBounds
	}

	return self.segs[id], nil
}

func (self *MmapArena) Allocate(minsz capnp.Size, segs map[capnp.SegmentID]*capnp.Segment) (capnp.SegmentID, []byte, error) {
	segid := capnp.SegmentID(len(self.segs))
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED
	length := capnp.Size(nearestPage(int(minsz))) // TODO

	syscall.Ftruncate(int(self.file.Fd()), int64(self.off)+int64(length))

	b, err := syscall.Mmap(int(self.file.Fd()), self.off, int(length), prot, flags)
	if err != nil {
		log.Panicln(err)
		return 0, nil, err
	}
	seg := b[:0]

	self.off += int64(length)
	self.segs[segid] = seg
	self.maps = append(self.maps, b)

	return segid, seg, nil
}

func (self *MmapArena) Close() error {
	for _, b := range self.maps {
		b = b[:cap(b)] // HACK
		err := syscall.Munmap(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *MmapArena) MarshalHeader() []byte {
	sizes := make([]int, len(self.segs))
	for i, b := range self.segs {
		sizes[i] = int(cap(b))
	}

	return marshalStreamHeader(sizes)
}

func nearestPage(len int) int {
	remainder := len % pageSize
	if remainder == 0 {
		return len
	} else {
		return len + pageSize - remainder
	}
}

func main() {
	name := "test"
	fd, err := memfd_create(name, MFD_ALLOW_SEALING|MFD_CLOEXEC)
	if err != nil {
		panic(err)
	}

	f := os.NewFile(uintptr(fd), name)

	// Write message
	arena := NewMmapArena(f, 0)
	_, seg, err := capnp.NewMessage(arena)
	greeting, err := NewRootGreeting(seg)
	if err != nil {
		panic(err)
	}

	greeting.SetText("Hello, world!")

	hdr := arena.MarshalHeader()

	err = arena.Close()
	if err != nil {
		panic(err)
	}

	// Now read it back
	sizes, _, err := parseStreamHeader(bytes.NewReader(hdr))
	if err != nil {
		panic(err)
	}

	totalSize := 0
	for _, sz := range sizes {
		totalSize += sz
	}

	b, err := syscall.Mmap(int(f.Fd()), 0, totalSize, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}

	arena2, err := demuxArena(sizes, b)
	if err != nil {
		panic(err)
	}

	msg := &capnp.Message{Arena: arena2}

	greeting, err = ReadRootGreeting(msg)
	txt, err := greeting.Text()
	log.Println("Greeting:", txt)

	log.Println("DONE")
}

func memfd_create(name string, flags uint) (fd int, err error) {
	var fdNo uintptr
	var namePtr *byte
	namePtr, err = syscall.BytePtrFromString(name)
	if err != nil {
		return
	}
	fdNo, _, err = syscall.Syscall(SYS_MEMFD_CREATE, uintptr(unsafe.Pointer(namePtr)), uintptr(flags), 0)
	fd = int(fdNo)
	if fd != -1 {
		err = nil
	}
	return
}

// -----------------------------------------------------------------------------
// Stream header sizes.
const (
	msgHeaderSize = 4
	segHeaderSize = 4
	wordSize      = 8
)

// parseStreamHeader parses the header of the stream framing format.
func parseStreamHeader(reader io.Reader) (sizes []int, off int, err error) {
	var maxSegBuf [msgHeaderSize]byte
	read, err := io.ReadFull(reader, maxSegBuf[:])
	off += read
	if err != nil {
		return nil, off, err
	}
	maxSeg := binary.LittleEndian.Uint32(maxSegBuf[:])
	hdrSize := streamHeaderSize(maxSeg)
	segsBuf := make([]byte, hdrSize-msgHeaderSize)
	read, err = io.ReadFull(reader, segsBuf[:])
	off += read
	if err != nil {
		return nil, off, err
	}

	segs := make([]int, maxSeg+1)
	for i := range segs {
		s := binary.LittleEndian.Uint32(segsBuf[i*segHeaderSize:])
		segs[i] = int(s) * wordSize
	}

	return segs, off, nil
}

// slice `data` into segments of the given lengths (in bytes)
func demuxArena(sizes []int, data []byte) (capnp.Arena, error) {
	segs := make([][]byte, len(sizes))
	for i := range sizes {
		sz := sizes[i]
		segs[i], data = data[:sz:sz], data[sz:]
	}
	return capnp.MultiSegment(segs), nil
}

func marshalStreamHeader(sizes []int) []byte {
	b := make([]byte, streamHeaderSize(uint32(len(sizes)-1)))
	binary.LittleEndian.PutUint32(b, uint32(len(sizes)-1))
	for i, sz := range sizes {
		loc := msgHeaderSize + i*segHeaderSize
		binary.LittleEndian.PutUint32(b[loc:], uint32(sz/wordSize))
	}
	return b
}

// streamHeaderSize returns the size of the header, given the
// first 32-bit number.
func streamHeaderSize(n uint32) uint64 {
	return (msgHeaderSize + segHeaderSize*(uint64(n)+1) + 7) &^ 7
}
