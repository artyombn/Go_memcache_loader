package main

import "fmt"

func main() {
	sum := 0
	for i := 0; i < 10; i++ { // for i in range(10)
		sum += i
	}
	fmt.Println(sum)

	sum = 1
	for sum < 1000 {  // while sum < 1000:
	    sum += sum
	}
    fmt.Println(sum)

    for { // while True:
    fmt.Println("Infinity!")
    break // or return
    }

    fruits := []string{"apple", "banana", "cherry"}
    // for i, fruit in enumerate(fruits)
    for i, fruit := range fruits {
        fmt.Printf("%d: %s\n", i, fruit)
    }
    for _, fruit := range fruits {
        fmt.Println(fruit)
    }

}
