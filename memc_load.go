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
    "bufio"
    "strconv"
    "github.com/artyombn/Go_memcache_loader/appsinstalled"
    "github.com/bradfitz/gomemcache/memcache"
    "google.golang.org/protobuf/proto"
)

const normalErrRate = 0.01
type AppsInstalled struct {
    DevType string
    DevID   string
    Lat     float64
    Lon     float64
    Apps    []int
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
    ua := &appsInstalled.UserApps{
        Lat:    appsInstalled.lat,
        Lon:    appsInstalled.lon,
        Apps:   appsInstalled.apps,
    }

    key := fmt.Sprintf("%s:%s", appsInstalled.DevType, appsInstalled.DevID)
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
    devID := lineParts[1]
    lat := lineParts[2]
    lon := lineParts[3]
    rawApps := lineParts[4]

    if devType == "" || devID == "" {
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

    lat64, errLat := strconv.ParseFloat(lat, 32) // always return float64
    lon64, errLon := strconv.ParseFloat(lon, 32)
    if errLat != nil || errLon != nil {
		log.Printf("Invalid geo coords: `%s`", line)
		return nil
	}
	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     float32(lat64),
		Lon:     float32(lon64),
		Apps:    apps,
	}
}

func main() {
    idfa := flag.String("idfa", "", "idfa memcached address")
	gaid := flag.String("gaid", "", "gaid memcached address")
	adid := flag.String("adid", "", "adid memcached address")
	dvid := flag.String("dvid", "", "dvid memcached address")
	pattern := flag.String("pattern", "./data/*.tsv.gz", "file pattern")
	dryRun := flag.Bool("dry", false, "dry run mode")

	flag.Parse()

    deviceMemc := map[string]string{
		"idfa": *idfa,
		"gaid": *gaid,
		"adid": *adid,
		"dvid": *dvid,
	}

	files, err := filepath.Glob(*pattern)
	if err != nil {
		log.Fatalf("Failed to match pattern: %v", err)
	}

    for _, fn := range files {
		processed := 0
		errors := 0
		log.Printf("Processing %s", fn)

		fd, err := os.Open(fn)
		if err != nil {
			log.Printf("Can't open file %s:%v", fn, err)
			continue
		}
		defer fd.Close() // defer - run before func return

		gzReader, err := gzip.NewReader(fd)
		if err != nil {
			log.Printf("Can't create gzip reader %s:%v", fn, err)
			continue
		}
		defer gzReader.Close()

		scanner := bufio.NewScanner(gzReader) // scan each line via bufio
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			appsinstalled := parseAppsInstalled(line)
			if appsinstalled == nil {
				errors++
				continue
			}
			memcAddr, ok := deviceMemc[appsinstalled.DevType]
			if !ok {
				errors++
				log.Printf("Unknown device type: %s", appsinstalled.DevType)
				continue
			}
			ok = insertAppsInstalled(memcAddr, appsinstalled, *dryRun)
			if ok {
				processed++
			} else {
				errors++
			}
		}

		if processed == 0 {
			err := dotRename(fn)
            if err != nil {
                log.Printf("Something wrong with renaming: %s:%v", fn, err)
            }
			continue
		}

		errRate := float64(errors) / float64(processed)
		if errRate < normalErrRate {
			log.Printf("Acceptable error rate (%f). Successful load", errRate)
		} else {
			log.Printf("High error rate (%f > %f). Failed load", errRate, normalErrRate)
		}

        err := dotRename(fn)
        if err != nil {
            log.Println("Something wrong with renaming: %s:%v", fn, err)
        }
	}
}
