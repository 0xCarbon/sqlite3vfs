package sqlite3vfs

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestVFSRegisterVsCallbackRace exercises concurrent RegisterVFS (vfsMap
// write in newVFS) against in-flight VFS callbacks (vfsMap read in
// vfsFromC). Before the vfsMux fix this is a "concurrent map read and map
// write" (fatal, unrecoverable process crash) or a race-detector failure.
func TestVFSRegisterVsCallbackRace(t *testing.T) {
	vfs := newTempVFS()
	if err := RegisterVFS("racevfs", vfs); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", "file:racedb?vfs=racevfs")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id integer primary key)`); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer goroutine: keep registering VFSes (vfsMap writes).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := RegisterVFS(fmt.Sprintf("racevfs-w-%d", i), vfs); err != nil {
				t.Errorf("RegisterVFS: %v", err)
				return
			}
		}
	}()

	// Reader goroutines: drive VFS callbacks (vfsFromC reads).
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := db.Exec(`INSERT INTO t (id) VALUES (NULL)`); err != nil {
					t.Errorf("insert: %v", err)
					return
				}
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
					t.Errorf("query: %v", err)
					return
				}
			}
		}(g)
	}

	time.Sleep(750 * time.Millisecond)
	close(stop)
	wg.Wait()
}
