package main

import (
	"fmt"
	"math"
)

func main() {
	// constants
	const IMTPower = 2

	// variables
	var userHeight float64
	var userWeight float64

	fmt.Println("___ IMT calculator ___")
	fmt.Print("What is your Height in centimeters? ")
	fmt.Scan(&userHeight)
	fmt.Print("What is your Weight? ")
	fmt.Scan(&userWeight)

	IMT := userWeight / math.Pow(userHeight/100, IMTPower) // userHeight ^ IMTPower

	// fmt.Print("Your IMT: ")
	// fmt.Print(IMT)
	fmt.Printf("Your IMT: %v", IMT)
	fmt.Println("")
	fmt.Printf("Your IMT: %.0f", IMT)

	result := fmt.Sprintf("Your IMT: %.0f", IMT)
	fmt.Println("")
	fmt.Print(result)
}


