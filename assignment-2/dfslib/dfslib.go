/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
)

// -----------------------------------------------------------------------------

// Define types.

// A Chunk is the unit of reading/writing in DFS.
type Chunk [32]byte

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", e)
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	// - WriteModeTimeoutError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {
	// TODO
	// For now return LocalPathError
	return nil, LocalPathError(localPath)
}

// -----------------------------------------------------------------------------

// Helper functions.

// Returns true iff filename fname consists of 1-16 lowercase alphanumeric
// characters.
func CheckFilename(fname string) (ok bool) {

	length := len(fname)
	if length < 1 || length > 16 {
		return false
	}
	filenameChecker := regexp.MustCompile("^[a-z0-9]+$").MatchString

	return filenameChecker(fname)

}

// Creates a file if it does not exist already.
// Can return the following error(s):
// - LocalPathError
func CreateFile(fullPath string) (err error) {

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		f, err := os.Create(fullPath)
		if err != nil {
			return LocalPathError(fullPath)
		}
		f.Close()
	}

	return nil

}

// Returns the ID from a past connection to the server. If an ID does not exist,
// returns -1.
// Can return the following error(s):
// - LocalPathError
func FetchOldId(localPath string) (id int, err error) {

	id = -1
	fullPath := path.Join(localPath, "id.txt")

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		// If the ID file does not exist, create one.
		f, err := os.Create(fullPath)
		if err != nil {
			return id, LocalPathError(fullPath)
		}
		f.Close()
		return id, nil
	} else {
		// Otherwise, read the id from the file.
		f, err := os.Open(fullPath)
		if err != nil {
			return id, LocalPathError(fullPath)
		}
		defer f.Close()

		idBuf := make([]byte, 64)
		n, err := f.Read(idBuf)
		if err != nil {
			return id, LocalPathError(fullPath)
		}
		id, err := strconv.Atoi(string(idBuf[:n]))
		if err != nil {
			return id, LocalPathError(fullPath)
		}
		return id, nil
	}

}

// Stores given contents to a file on disk.
// Can return the following error(s):
// - LocalPathError
func StoreFileOnDisk(contents []byte, localPath, fname string) (err error) {

	fullPath := path.Join(localPath, fname+".dfs")

	err = CreateFile(fullPath)
	if err != nil {
		return LocalPathError(fullPath)
	}

	f, err := os.OpenFile(fullPath, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return LocalPathError(fullPath)
	}
	defer f.Close()

	_, err = f.Write(contents)
	if err != nil {
		return LocalPathError(fullPath)
	}
	f.Sync()

	return nil

}

// Writes a chunk to a file on disk.
func WriteChunk(localPath, fname string, chunk *Chunk, chunkNum uint8) (err error) {

	fullPath := path.Join(localPath, fname+".dfs")

	err = CreateFile(fullPath)
	if err != nil {
		return LocalPathError(fullPath)
	}

	f, err := os.OpenFile(fullPath, os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return LocalPathError(fullPath)
	}
	defer f.Close()

	_, err = f.WriteAt(chunk[:], int64(chunkNum))
	if err != nil {
		return LocalPathError(fullPath)
	}
	f.Sync()

	return nil

}

// -----------------------------------------------------------------------------
