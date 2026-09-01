package wal

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestWALCrashHelper is run in a child test process. The parent kills that
// process after observing acknowledged Append calls, which makes this an
// actual process-crash test rather than an in-process simulation.
func TestWALCrashHelper(t *testing.T) {
	if os.Getenv("LATTICE_WAL_CRASH_HELPER") != "1" {
		return
	}
	log, err := Open(os.Getenv("LATTICE_WAL_CRASH_PATH"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	for n := 0; n < 1000; n++ {
		if err := log.Append(Record{Op: "put", Key: fmt.Sprintf("key-%d", n), Value: []byte("acknowledged")}); err != nil {
			t.Fatal(err)
		}
		// The parent watches stdout as the acknowledgement boundary.
		fmt.Fprintln(os.Stdout, n)
		time.Sleep(time.Millisecond)
	}
}

func TestWALCrashRecovery(t *testing.T) {
	for run := 0; run < 100; run++ {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")
		command := exec.Command(os.Args[0], "-test.run=^TestWALCrashHelper$", "-test.v")
		command.Env = append(os.Environ(),
			"LATTICE_WAL_CRASH_HELPER=1",
			"LATTICE_WAL_CRASH_PATH="+path,
		)
		stdout, err := command.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(stdout)
		acknowledged := 0
		for scanner.Scan() {
			if _, err := strconv.Atoi(scanner.Text()); err != nil {
				continue // Ignore the Go test runner's verbose lines.
			}
			acknowledged++
			if acknowledged == 25 {
				if err := command.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if acknowledged != 25 {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("run %d observed %d acknowledged appends before child exited", run, acknowledged)
		}
		_ = command.Wait()

		log, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		records, err := log.Replay()
		_ = log.Close()
		if err != nil {
			t.Fatalf("run %d replay: %v", run, err)
		}
		if len(records) < acknowledged {
			t.Fatalf("run %d recovered %d records, want at least %d acknowledged records", run, len(records), acknowledged)
		}
		for n := 0; n < acknowledged; n++ {
			if records[n].Key != fmt.Sprintf("key-%d", n) {
				t.Fatalf("run %d record %d = %q, want key-%d", run, n, records[n].Key, n)
			}
		}
	}
}
