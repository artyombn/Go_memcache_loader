package main

import (
	"fmt"
	"math"
)

func main() {
	// constants
	const IMTPower = 2

	// variables
	userHeight := 1.8
	var userWeight float64 = 100
	IMT := userWeight / math.Pow(userHeight, IMTPower) // userHeight ^ IMTPower
	fmt.Print(IMT)
}


