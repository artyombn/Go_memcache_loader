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
type AppsInstalled struct {
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

func insertAppsInstalled(memcAddr string, appsInstalled AppsInstalled, dryRun bool) bool {
    ua := appsInstalled.UserApps{
        Lat:    appsInstalled.lat,
        Lon:    appsInstalled.lon,
        Apps:   appsInstalled.apps,
    }

    key := fmt.Sprintf("%s:%s", appsInstalled.devType, appsInstalled.devID)
    packed, err := proto.Marshal(ua) // serializing to []byte
	if err != nil {
		log.Printf("Failed to serialize: %v", err)
		return false
	}

    if dryRun {
        log.Printf("%s - %s -> %s", memcAddr, key, ua.String())
    } else {
        memc := memcache.New(memcAddr)
        err := memc.Set(&memcache.Item{Key: key, Value: packed})
        if err != nil {
            log.Printf("Cannot write to memc %s: %v", memcAddr, err)
            return false
        }
    }
    return true
}

// err := dotRename("logs/data.txt")
// if err != nil {
//     log.Println("Something wrong with renaming:", err)
// }


func main() {
    fmt.Println("This is main func")
}