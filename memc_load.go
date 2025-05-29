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
    "strings"
    "bufio"
    "strconv"
    "time"
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
    Apps    []uint32
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
    ua := &appsinstalled.UserApps{
        Lat:    proto.Float64(appsInstalled.Lat),
        Lon:    proto.Float64(appsInstalled.Lon),
        Apps:   appsInstalled.Apps,
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

    var apps []uint32

    rawAppItems := strings.Split(rawApps, ",")
    for _, app := range rawAppItems {
        app = strings.TrimSpace(app)
        if app == "" {
			continue
		}
        if id, err := strconv.ParseUint(app, 10, 32); err == nil { // from str to int
			apps = append(apps, uint32(id))
		} else {
			log.Printf("Not all user apps are digits: `%s`", line)
		}
    }

    lat64, errLat := strconv.ParseFloat(lat, 64) // always return float64
    lon64, errLon := strconv.ParseFloat(lon, 64)
    if errLat != nil || errLon != nil {
		log.Printf("Invalid geo coords: `%s`", line)
		return nil
	}
	return &AppsInstalled{
		DevType: devType,
		DevID:   devID,
		Lat:     lat64,
		Lon:     lon64,
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
	logFile := flag.String("log", "", "log file path")

	flag.Parse()

	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening log file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	if *dryRun {
		log.SetPrefix("DEBUG ")
	} else {
		log.SetPrefix("INFO ")
	}

    log.Printf("Memc loader started with pattern: %s", *pattern)

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

    var filteredFiles []string
    for _, f := range files {
        _, fn := filepath.Split(f)
        if strings.HasPrefix(fn, ".") {
            continue
        }
        filteredFiles = append(filteredFiles, f)
    }

    startTime := time.Now()
    totalProcessed := 0
    totalErrors := 0

    for _, fn := range filteredFiles {
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
			parsed := parseAppsInstalled(line)
			if parsed == nil {
				errors++
				continue
			}
			memcAddr, ok := deviceMemc[parsed.DevType]
			if !ok {
				errors++
				log.Printf("Unknown device type: %s", parsed.DevType)
				continue
			}
			ok = insertAppsInstalled(memcAddr, *parsed, *dryRun)
			if ok {
				processed++
			} else {
				errors++
			}
		}

		if processed == 0 {
			err = dotRename(fn)
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

        err = dotRename(fn)
        if err != nil {
            log.Println("Something wrong with renaming: %s:%v", fn, err)
        }

        totalProcessed += processed
        totalErrors += errors
	}

    endTime := time.Now()
    totalTime := endTime.Sub(startTime).Seconds()
    log.Printf("Total processed: %d, total errors: %d, total execution time: %f sec", totalProcessed, totalErrors, totalTime)
}
