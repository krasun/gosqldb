package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
)

// Only one db process is allowed to run within the specified db directory.
// The synchronization is implemented with a "lock" file.
// If the file exists, a process can not start, otherwise
// it starts and creates the file, after the process has done
// its job. It must remove the file regardless of successful
// or failed execution.
const lockFileName = "gosqldb.lock"

func removeLockFile(lockFilePath string) {
	if err := os.Remove(lockFilePath); err != nil {
		log.Fatalf("failed to remove lock file %s: %s", lockFilePath, err)
	}
}

func main() {
	dbDir := ""
	if len(os.Args) < 2 {
		log.Fatalf("path to the db directory is required")
	}

	dbDir = os.Args[1]
	log.Printf("db directory path: %s", dbDir)

	lockFilePath := path.Join(dbDir, lockFileName)
	lockFile, err := os.OpenFile(lockFilePath, os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Fatalf("failed to create lock file %s: %s", lockFilePath, err)
	}

	err = lockFile.Close()
	if err != nil {
		log.Fatalf("failed to close lock file %s: %s", lockFilePath, err)
	}
	log.Printf("lock file %s created\n", lockFilePath)
	defer removeLockFile(lockFilePath)

	db, err := NewDatabase(dbDir)
	if err != nil {
		log.Fatalf("failed to instantiate database: %s", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			removeLockFile(lockFilePath)
			os.Exit(0)
		}
	}()

	http.HandleFunc("/", handler(db))

	log.Println("listening incoming requests at :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
