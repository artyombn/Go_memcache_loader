package main

import (
	"fmt"
	"errors"
)

func main() {
    var ErrSomething = errors.New("Only positive figures")

    fmt.Println("Input two values")

	valueFirst, valueSecond := inputData()

    if valueFirst < 0 || valueSecond < 0 {
	    fmt.Println("Error:", ErrSomething)
	    return
    }

    division, err := divisionValues(valueFirst, valueSecond)
    if err != nil {
        fmt.Println("Error:", err)
        return
    }

	outputResult(division)
}

func inputData() (float64, float64) {
    var first float64
    var second float64

	fmt.Print("Write the first value: ")
	fmt.Scan(&first)
	fmt.Print("Write the second one: ")
	fmt.Scan(&second)
	return first, second
}

func divisionValues(first, second float64) (float64, error) {
    if second == 0 {
        return 0, errors.New("Division by ZERO")
    }
    return first / second, nil
}

func outputResult(result float64) {
    output := fmt.Sprintf("Division result: %.2f", result)
	fmt.Println(output)
}
