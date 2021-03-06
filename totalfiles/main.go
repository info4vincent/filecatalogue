package main

import (
	"crypto/sha1"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// 8KB
const filechunk = 8192

// CollectionInfo Contains information of a disk and a reference to a backup disk
type CollectionInfo struct {
	DBName           string
	FullRootName     string
	CollectionName   string
	BackupCollection string
}

// Collection CollectionMetaData of disk
var Collection = CollectionInfo{DBName: "filecatalogue", FullRootName: "e:/Testme", CollectionName: "TestMe", BackupCollection: "TestMe2"}

// NodeInfo a file node meta information
type NodeInfo struct {
	FullName       string
	Name           string
	FileSize       int64
	Sha1           string
	BackupLocation string
}

// Collections Gets Collections to process or gives initial collection
func Collections(session *mgo.Session) []CollectionInfo {
	c := session.DB(Collection.DBName).C("Settings")

	var collections []CollectionInfo
	err := c.Find(nil).All(&collections)
	if (err == nil || err.Error() != "not found") && (len(collections) > 0) {
		return collections
	} else if (err != nil && err.Error() == "not found") || (err == nil && len(collections) == 0) {
		err2 := c.Insert(Collection)
		if err2 != nil {
			log.Fatal(err2)
		}
		collections = Collections(session)
	} else {
		log.Fatal(err)
	}
	return collections
}

func findBackup(session *mgo.Session, nodeInfo *NodeInfo) {
	c2 := session.DB(Collection.DBName).C(Collection.BackupCollection)

	var result NodeInfo
	err2 := c2.Find(bson.M{"sha1": nodeInfo.Sha1}).One(&result)

	if err2 == nil || err2.Error() != "not found" {
		nodeInfo.BackupLocation = fmt.Sprintf("%s@%s", Collection.BackupCollection, result.FullName)
	} else if err2 != nil && err2.Error() == "not found" {
		nodeInfo.BackupLocation = ""
	} else {
		log.Fatal(err2)
	}
}

func writeToDb(session *mgo.Session, nodeInfo NodeInfo) {
	c := session.DB(Collection.DBName).C(Collection.CollectionName)

	_, err := c.UpsertId(nodeInfo.FullName, &nodeInfo)
	if err != nil {
		log.Fatal(err)
	}
}

// TraverseDir Search in dir and searches for files.
func TraverseDir(session *mgo.Session, dirName string) {
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		newDir := dirName + "/" + file.Name()
		fullFile, _ := filepath.Abs(newDir)
		fmt.Println(fullFile)
		if file.IsDir() {
			TraverseDir(session, fullFile)
		} else {
			calcSHA1(session, fullFile)
		}
	}
}

func calcSHA1(session *mgo.Session, aFile string) {

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

	fullfileName := strings.Replace(aFile, "\\", "/", -1)
	nodeInfo := NodeInfo{FullName: fullfileName, Name: info.Name(), FileSize: filesize}
	nodeInfo.Sha1 = fmt.Sprintf("%x", hash.Sum(nil))

	findBackup(session, &nodeInfo)

	writeToDb(session, nodeInfo)
}

func connectDB() *mgo.Session {
	uri := os.Getenv("MONGODB_URL")
	if uri == "" {
		fmt.Println("No connection string provided - set MONGODB_URL = mongodb://{user}:{password}@mongodb.documents.azure.com:{port}")
		os.Exit(1)
	}
	uri = strings.TrimSuffix(uri, "?ssl=true")

	tlsConfig := &tls.Config{}
	tlsConfig.InsecureSkipVerify = true

	dialInfo, err := mgo.ParseURL(uri)

	if err != nil {
		fmt.Println("Failed to parse URI: ", err)
		os.Exit(1)
	}

	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
		return conn, err
	}

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		fmt.Println("Failed to connect: ", err)
		os.Exit(1)
	}

	dbnames, err := session.DB("").CollectionNames()
	if err != nil {
		fmt.Println("Couldn't query for collections names: ", err)
		os.Exit(1)
	}

	fmt.Println(dbnames)

	return session
}

func main() {
	session := connectDB()

	defer session.Close()

	for _, collection := range Collections(session) {
		Collection = collection
		TraverseDir(session, collection.FullRootName)
	}
}
