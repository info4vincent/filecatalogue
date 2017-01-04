package main

import (
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
)

// 8KB
const filechunk = 8192

func TraverseDir(dirName string) {
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		newDir := dirName + "/" + file.Name()
		fullFile, _ := filepath.Abs(newDir)
		fmt.Println(fullFile)
		if file.IsDir() {
			TraverseDir(fullFile)
		} else {
			calcSHA1(fullFile)
		}
	}
}

func calcSHA1(aFile string) {

	// Open the file for reading
	file, err := os.Open(aFile)
	if err != nil {
		fmt.Println("Cannot find file:", aFile)
		return
	}

	defer file.Close()

	// Get file info
	info, err := file.Stat()
	if err != nil {
		fmt.Println("Cannot access file:", aFile)
		return
	}

	// Get the filesize
	filesize := info.Size()

	// Calculate the number of blocks
	blocks := uint64(math.Ceil(float64(filesize) / float64(filechunk)))

	// Start hash
	//hash := md5.New()
	hash := sha1.New()

	// Check each block
	for i := uint64(0); i < blocks; i++ {
		// Calculate block size
		blocksize := int(math.Min(filechunk, float64(filesize-int64(i*filechunk))))

		// Make a buffer
		buf := make([]byte, blocksize)

		// Make a buffer
		file.Read(buf)

		// Write to the buffer
		io.WriteString(hash, string(buf))
	}

	// Output the results
	fmt.Printf("%x\n", hash.Sum(nil))
}

func main() {
	TraverseDir("e:/GoogleDrive")
}
