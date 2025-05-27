package main

import (
    "fmt"
)

type petData struct {
    petName string
    petBreed string
    petColor string
    petAge int
}

type userData struct {
    userName string
    firstName string
    lastName string
    userAge int
    userPets []petData
}

func main() {
    pets := []petData{
        {petName: "Buddy", petBreed: "Labrador", petColor: "Yellow", petAge: 3},
        {petName: "Milo", petBreed: "Beagle", petColor: "Tricolor", petAge: 2},
    }

    user := userData{
        userName:   "Vanya5432",
        firstName:  "Ivan",
        lastName:   "Ivanov",
        userAge:    30,
        userPets:   pets,
    }

    fmt.Println(pets)
    fmt.Println(user)

    user.userName = "Ivan324354"
    user.userAge = 31

    fmt.Printf("Updated user: %s", user)
}
