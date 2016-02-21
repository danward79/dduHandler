package dduHandler

import (
	"bufio"
	"fmt"
	"io"
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

	go func(path string) error {

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

			r := bufio.NewReader(f)

			for {
				l, p, err := r.ReadLine()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("Readline Error: %v", err)
				}
				fmt.Println("Prefix: ", p)
				lineCount++
				chOut <- l
			}
		}

		fmt.Println("Files Processed:", fileCount, ", Lines Processed:", lineCount)

		chDone <- struct{}{}
		close(chOut)

		return nil
	}(path)

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
