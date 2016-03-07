package dduHandler

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

//LineScanner provides a channel of lines from the imported files
func LineScanner(path string, wg *sync.WaitGroup) (chan []byte, chan struct{}, error) {
	wg.Add(1)
	chOut := make(chan []byte)
	chDone := make(chan struct{})
	var fileCount, lineCount int

	//go scan(path, chOut, chDone, wg)
	go func(path string, chOut chan []byte) error {

		defer wg.Done()
		files := listFiles(path)

		for _, fileInfo := range files {
			fmt.Println(fileInfo.Name())
			fileCount++

			filePath := filepath.Join(path, fileInfo.Name())

			f, err := os.Open(filePath)
			if err != nil {
				log.Printf("Open DDU file error: %v", err)
			}

			scanner := bufio.NewScanner(f)
			if strings.HasSuffix(f.Name(), ".gz") {
				ar, err := gzip.NewReader(f)
				if err != nil {
					//return err
				}
				scanner = bufio.NewScanner(ar)
			}
			for {

				for scanner.Scan() {
					l := scanner.Text()
					chOut <- []byte(l)

				}
				if err := scanner.Err(); err != nil {
					log.Printf("Scanner error: %v", err)
				}

				lineCount++
				f.Close()
				break
			}

		}

		log.Println("Files Processed:", fileCount, ", Lines Processed:", lineCount)

		close(chOut)
		chDone <- struct{}{}

		return nil
	}(path, chOut)

	return chOut, chDone, nil
}

//scan
func scan(path string, chOut chan []byte, chDone chan struct{}, wg *sync.WaitGroup) error {
	var fileCount, lineCount int

	defer wg.Done()
	files := listFiles(path)
	fmt.Println("Files: ", len(files))

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
			if strings.HasSuffix(f.Name(), ".gz") {
				ar, err := gzip.NewReader(f)
				if err != nil {
					return err
				}
				scanner = bufio.NewScanner(ar)
			}

			for scanner.Scan() {
				l := scanner.Text()
				chOut <- []byte(l)

			}

			lineCount++

		}
	}

	fmt.Println("Files Processed:", fileCount, ", Lines Processed:", lineCount)

	chDone <- struct{}{}
	close(chOut)

	return nil
}

//listFiles provides a list of entities in a path provided.
func listFiles(path string) []os.FileInfo {
	fileList, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatalln(err)
	}
	return fileList
}
