package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

func main() {
	var kva []KeyValue
	oname := "TTEESSTT"
	ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	kvv := KeyValue{"sert", "1"}
	enc := json.NewEncoder(ofile)
	enc.Encode(&kvv)
	enc.Encode(&kvv)
	enc.Encode(&kvv)
	enc.Encode(&kvv)
	enc.Encode(&kvv)
	ofile.Close()

	OFile, _ := os.OpenFile(oname, os.O_RDONLY, 0644)
	dec := json.NewDecoder(OFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			fmt.Printf("Failed to decode file: %s\n", oname)
			break
		}
		kva = append(kva, kv)
	}
	fmt.Println("aa")

}
