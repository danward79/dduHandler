package dduHandler

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

//LineScanner provides a channel of lines from the imported files
func LineScanner(path string, wg *sync.WaitGroup) (chan []byte, chan struct{}, error) {
	wg.Add(1)
	chOut := make(chan []byte)
	chDone := make(chan struct{})
	var fileCount, lineCount int

	go func(path string, chOut chan []byte, chDone chan struct{}) error {

		defer wg.Done()
		files := listFiles(path)

		for _, fileInfo := range files {
			fmt.Println(fileInfo.Name())
			fileCount++

			filePath := filepath.Join(path, fileInfo.Name())

			f, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("Open DDU file error: %v", err)
			}
			defer f.Close()

			for {

				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					l := scanner.Text()
					chOut <- []byte(l)

				}
				if err := scanner.Err(); err != nil {
					fmt.Fprintln(os.Stderr, "reading standard input:", err)
				}

				lineCount++

			}
		}

		fmt.Println("Files Processed:", fileCount, ", Lines Processed:", lineCount)

		chDone <- struct{}{}
		close(chOut)

		return nil
	}(path, chOut, chDone)

	return chOut, chDone, nil
}

//listFiles provides a list of entities in a path provided.
func listFiles(path string) []os.FileInfo {
	fileList, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatalln(err)
	}
	return fileList
}
