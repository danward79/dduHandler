package dduHandler

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

//DDUParams ...
type DDUParams struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

//DDUData ...
type DDUData struct {
	Params []DDUParams `xml:"params>param"`
}

//Cycle door reading
type Cycle struct {
	Vehicle    string
	Position   string
	Type       string
	Operation  int64
	Time       time.Time
	Duration   int
	Profile    string
	IOProfile  string
	RefProfile []int64
}

//LineDecode ...
func LineDecode(chIn chan []byte, chDone chan struct{}, wg *sync.WaitGroup) (chan Cycle, error) {
	wg.Add(1)
	chOut := make(chan Cycle)

	go func(chIn chan []byte, chOut chan Cycle) {
		defer wg.Done()
		var c Cycle

		for {
			select {
			case l := <-chIn:
				var d DDUData
				err := xml.Unmarshal(l, &d)
				if err != nil && err != io.EOF {
					fmt.Println("Line: ", string(l))
					log.Fatalf("XML Unmarshall Err: %v", err)

				}

				for _, v := range d.Params {

					switch {
					case v.Name == "Door ID":

						v := strings.Split(v.Value, " ")
						c.Vehicle = v[1]
						c.Position = v[2]
						continue

					case v.Name == "csvdata":

						values, err := readCSV(v.Value)
						if err != nil {
							log.Fatal(err)
						}

						if len(values) == 24 {
							c.Type = values[0]
							c.Operation = convStringInt(values[1])

							t, err := time.Parse("02-01-2006 15:04:05", values[2])
							if err != nil {
								log.Fatal(err)
							}

							c.Time = t.Local()

							c.Duration = len(values[7]) * 19
							c.Profile = values[7]
							c.IOProfile = values[8]
							c.RefProfile = convRefProfile(values[9:19])

							chOut <- c
						}

					}
				}

			case <-chDone:
				return
			}
		}

	}(chIn, chOut)
	return chOut, nil
}

//readCSV
func readCSV(csvdata string) ([]string, error) {
	//fmt.Println(csvdata)
	r := csv.NewReader(strings.NewReader(csvdata))

	data, err := r.Read()
	if err == io.EOF {
		return data, nil
	}
	if err != nil {
		return nil, fmt.Errorf("CSV Data Reader Error: %v", err)
	}

	return data, nil
}

//convStringInt
func convStringInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		log.Fatal(err)
	}
	return i
}

//convRefProfile
func convRefProfile(v []string) []int64 {
	var p []int64
	for _, v := range v {
		p = append(p, convStringInt(v))
	}
	return p
}
