package main

import (
	"fmt"
	"math"
)

func main() {
    fmt.Println("___ IMT calculator ___")

	userHeight, userWeight := inputData()
    IMT := calculateIMT(userHeight, userWeight)
	outputResult(IMT)
}

func inputData() (float64, float64) {
    var height float64
    var weight float64

	fmt.Print("What is your Height in centimeters? ")
	fmt.Scan(&height)
	fmt.Print("What is your Weight? ")
	fmt.Scan(&weight)
	return height, weight
}

func calculateIMT(height float64, weight float64) float64 {
	const IMTPower = 2
    IMT := weight / math.Pow(height/100, IMTPower) // userHeight ^ IMTPower
    return IMT
}

func outputResult(imt float64) {
    result := fmt.Sprintf("Your IMT: %.0f", imt)
	fmt.Print(result)
}
