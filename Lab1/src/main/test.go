package main

import "fmt"

func main() {
	cc := make([]string, 0)
	cc = append(cc, "aa")
	for _, value := range cc {
		fmt.Println(value)
	}

}
