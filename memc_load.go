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

func parseAppsInstalled(line string) *AppsInstalled {
// *AppsInstalled -> return structure pointer
// AppsInstalled -> return structure itself
    lineParts := strings.Split(strings.TrimSpace(line), "\t") // TrimSpace - remove spaces
    if len(lineParts) < 5 {
        return nil
    }
    devType := lineParts[0]
    devId := lineParts[1]
    Lat := lineParts[2]
    Lon := lineParts[3]
    rawApps := lineParts[4]

    if devType == "" || devId == "" {
        return nil
    }

    var apps []int32

    rawAppItems := strings.Split(rawApps, ",")
    for _, app := range rawAppItems {
        app = strings.TrimSpace(app)
        if app == "" {
			continue
		}
        if id, err := strconv.Atoi(app); err == nil { // from str to int
			apps = append(apps, int32(id))
		} else {
			log.Printf("Not all user apps are digits: `%s`", line)
		}
    }

    Lat64, errLat := strconv.ParseFloat(Lat, 32) // always return float64
    Lon64, errLon := strconv.ParseFloat(Lon, 32)
    if errLat != nil || errLon != nil {
		log.Printf("Invalid geo coords: `%s`", line)
		return nil
	}
	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     float32(Lat64),
		Lon:     float32(Lon64),
		Apps:    apps,
	}
}

// err := dotRename("logs/data.txt")
// if err != nil {
//     log.Println("Something wrong with renaming:", err)
// }


func main() {
    fmt.Println("This is main func")
}