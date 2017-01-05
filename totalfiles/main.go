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

	fullfileName := strings.Replace(aFile, "\\", "/", -1)
	nodeInfo := NodeInfo{FullName: fullfileName, Name: info.Name(), FileSize: filesize}
	nodeInfo.Sha1 = fmt.Sprintf("%x", hash.Sum(nil))

	writeToDb(session, nodeInfo)
}

func connectDB() *mgo.Session {
	//uri := os.Getenv("MONGODB_URL")
	uri := "mongodb://mongodb1584:YXo7XJZu4uT22axJ0ceECSgsggh4SpwFDhKHU6wy2nIW28gEwMqi78UEEo9cbfY265y9ABoYLC2nWdEV2JvjqQ==@mongodb1584.documents.azure.com:10250"
	if uri == "" {
		fmt.Println("No connection string provided - set MONGODB_URL")
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

	TraverseDir(session, Collection.FullRootName)

	Collection = CollectionInfo{DBName: "filecatalogue", FullRootName: "e:/Testme2", CollectionName: "TestMe2", BackupCollection: "TestMe"}

	TraverseDir(session, Collection.FullRootName)
}
