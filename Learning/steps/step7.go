package main

import (
    "fmt"
    "time"
)

func main() {
    ch := make(chan string)
    exit := make(chan int)

    fmt.Println("Hello")
    go say("world", ch, exit)
//     from channel (value: <-ch)
//     into channel (ch <- value)
//     reading from channel stops main loop
    v := <-ch
    fmt.Println(v)

    for i := range exit {
        fmt.Println(i)
    }
}

func say(word string, ch chan string, exit chan int) {
    fmt.Println(word)
    ch <- "exit"
    for i:= 0; i< 5; i++ {
        time.Sleep(100 * time.Millisecond)
        exit <- i
    }
    close(exit)
}

func selectOne() {

}