// Package wal implements a small append-only, checksummed write-ahead log.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	ErrCorrupt = errors.New("wal: corrupt record")
	magic      = uint32(0x4c415431) // LAT1
)

type Record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

type Log struct {
	mu   sync.Mutex
	file *os.File
}

func Open(path string) (*Log, error) {
	if err := os.MkdirAll(filepathDir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{file: f}, nil
}

func (l *Log) Append(r Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > int(^uint32(0)) {
		return fmt.Errorf("wal: record too large")
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], magic)
	binary.BigEndian.PutUint32(header[4:8], uint32(len(b)))
	checksum := crc32.ChecksumIEEE(b)
	var tail [4]byte
	binary.BigEndian.PutUint32(tail[:], checksum)
	if _, err := l.file.Write(header[:]); err != nil {
		return err
	}
	if _, err := l.file.Write(b); err != nil {
		return err
	}
	if _, err := l.file.Write(tail[:]); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *Log) Replay() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(l.file)
	var records []Record
	for {
		var header [8]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // An interrupted final append is safely ignored.
			}
			return nil, err
		}
		if binary.BigEndian.Uint32(header[0:4]) != magic {
			return nil, ErrCorrupt
		}
		n := binary.BigEndian.Uint32(header[4:8])
		if n > 128<<20 {
			return nil, fmt.Errorf("wal: record length %d exceeds limit", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		var tail [4]byte
		if _, err := io.ReadFull(reader, tail[:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		if binary.BigEndian.Uint32(tail[:]) != crc32.ChecksumIEEE(payload) {
			return nil, ErrCorrupt
		}
		var record Record
		if err := json.Unmarshal(payload, &record); err != nil {
			return nil, fmt.Errorf("wal: decode record: %w", err)
		}
		record.Value = append([]byte(nil), record.Value...)
		records = append(records, record)
	}
	_, err := l.file.Seek(0, io.SeekEnd)
	return records, err
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

func filepathDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == os.PathSeparator {
			if i == 0 {
				return string(path[:1])
			}
			return path[:i]
		}
	}
	return "."
}
