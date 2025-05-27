package main

// Python imports
// import os
// import gzip
// import sys
// import glob
// import logging
// import collections
// from optparse import OptionParser
// import appsinstalled_pb2
// import memcache

import (
    "os"
    "compress/gzip"
    "path/filepath"
    "log"
    "flag"
    "fmt"
    "io"
    "strings"
    "github.com/artyombn/Go_memcache_loader/appsinstalled"
    "github.com/bradfitz/gomemcache/memcache"
)

const normalErrRate = 0.01
type appsInstalled struct {
    devType string
    devID   string
    lat     float64
    lon     float64
    apps    []int
}

func dotRename(path string) error {
    head, fn := filepath.Split(path)
    newPath := filepath.Join(head, "." + fn)
    if err := os.Rename(path, newPath); err != nil {
        return fmt.Errorf("renaming failed %s: %w", path, err)
    }
    return nil
}

// err := dotRename("logs/data.txt")
// if err != nil {
//     log.Println("Something wrong with renaming:", err)
// }


func main() {
    fmt.Println("This is main func")
}