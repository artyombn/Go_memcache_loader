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
    "sync"
    "github.com/artyombn/Go_memcache_loader/appsinstalled"
    "github.com/bradfitz/gomemcache/memcache"
    "google.golang.org/protobuf/proto"
)

const normalErrRate = 0.01
const batchSize = 1000

type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

type ProcessResult struct {
	File      string
	Processed int
	Errors    int
	StartTime time.Time
	EndTime   time.Time
	Error     error
}

func dotRename(path string) error {
	head, fn := filepath.Split(path)
	newPath := filepath.Join(head, "." + fn)
	if err := os.Rename(path, newPath); err != nil {
		return fmt.Errorf("renaming failed %s: %w", path, err)
	}
	return nil
}

func insertAppsInstalledBatch(memc *memcache.Client, batch []AppsInstalled, memcAddr string, dryRun bool) (bool, int) {
	if len(batch) == 0 {
		return true, 0
	}

	items := make([]*memcache.Item, 0, len(batch))
	for _, appInst := range batch {
		ua := &appsinstalled.UserApps{
			Lat:  proto.Float64(appInst.Lat),
			Lon:  proto.Float64(appInst.Lon),
			Apps: appInst.Apps,
		}

		key := fmt.Sprintf("%s:%s", appInst.DevType, appInst.DevID)
		packed, err := proto.Marshal(ua)
		if err != nil {
			log.Printf("Failed to serialize: %v", err)
			continue
		}

		items = append(items, &memcache.Item{
			Key:   key,
			Value: packed,
		})
	}

	if dryRun {
		for _, appInst := range batch {
			ua := &appsinstalled.UserApps{
				Lat:  proto.Float64(appInst.Lat),
				Lon:  proto.Float64(appInst.Lon),
				Apps: appInst.Apps,
			}
			key := fmt.Sprintf("%s:%s", appInst.DevType, appInst.DevID)
			log.Printf("%s - %s -> %s", memcAddr, key, ua.String())
		}
		return true, len(batch)
	}

// Set operations to write item to memcache
	successCount := 0
	for _, item := range items {
		if err := memc.Set(item); err != nil {
			log.Printf("Cannot write to memc %s: %v", memcAddr, err)
		} else {
			successCount++
		}
	}

	return successCount == len(items), len(batch)
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

func worker(file string, memcClients map[string]*memcache.Client, memcAddrs map[string]string, dryRun bool) ProcessResult {
	result := ProcessResult{
		File:      file,
		StartTime: time.Now(),
	}

	log.Printf("Processing %s", file)

// Batches for each app
	batches := make(map[string][]AppsInstalled)
	for devType := range memcClients {
		batches[devType] = make([]AppsInstalled, 0, batchSize)
	}

	fd, err := os.Open(file)
	if err != nil {
		log.Printf("Can't open file %s: %v", file, err)
		result.Error = err
		result.EndTime = time.Now()
		return result
	}
	defer fd.Close() // defer - run before func return

	gzReader, err := gzip.NewReader(fd)
	if err != nil {
		log.Printf("Can't create gzip reader %s: %v", file, err)
		result.Error = err
		result.EndTime = time.Now()
		return result
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
			result.Errors++
			continue
		}

		devType := parsed.DevType
		if _, ok := memcClients[devType]; !ok {
			result.Errors++
			log.Printf("Unknown device type: %s", devType)
			continue
		}

		batches[devType] = append(batches[devType], *parsed)

		if len(batches[devType]) >= batchSize {
			ok, count := insertAppsInstalledBatch(memcClients[devType], batches[devType], memcAddrs[devType], dryRun)
			if ok {
				result.Processed += count
			} else {
				result.Errors += count
			}
			batches[devType] = batches[devType][:0] // clear slice
		}
	}

	for devType, batch := range batches {
		if len(batch) > 0 {
			ok, count := insertAppsInstalledBatch(memcClients[devType], batch, memcAddrs[devType], dryRun)

			if ok {
				result.Processed += count
			} else {
				result.Errors += count
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading file %s: %v", file, err)
		result.Error = err
	}

	if result.Processed == 0 {
		if err := dotRename(file); err != nil {
			log.Printf("Something wrong with renaming: %s: %v", file, err)
		}
	} else {
		errRate := float64(result.Errors) / float64(result.Processed)
		if errRate < normalErrRate {
			log.Printf("Acceptable error rate (%f). Successful load", errRate)
		} else {
			log.Printf("High error rate (%f > %f). Failed load", errRate, normalErrRate)
		}

		if err := dotRename(file); err != nil {
			log.Printf("Something wrong with renaming: %s: %v", file, err)
		}
	}

	result.EndTime = time.Now()
	return result
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

	memcClients := map[string]*memcache.Client{
		"idfa": memcache.New(*idfa),
		"gaid": memcache.New(*gaid),
		"adid": memcache.New(*adid),
		"dvid": memcache.New(*dvid),
	}

	memcAddrs := map[string]string{
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

	if len(filteredFiles) == 0 {
		log.Printf("No files found matching pattern %s", *pattern)
		return
	}

	startTime := time.Now()
	totalProcessed := 0
	totalErrors := 0

	results := make(chan ProcessResult, len(filteredFiles))
	var wg sync.WaitGroup

	maxWorkers := len(filteredFiles)
	if maxWorkers > 10 {
		maxWorkers = 10
	}

	semaphore := make(chan struct{}, maxWorkers)

	for _, file := range filteredFiles {
		wg.Add(1)
        // Goroutines to handle files
		go func(f string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			result := worker(f, memcClients, memcAddrs, *dryRun)
			results <- result
		}(file)
	}

	// Close channel after all workers
	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		totalProcessed += result.Processed
		totalErrors += result.Errors

		executionTime := result.EndTime.Sub(result.StartTime).Seconds()
		if result.Error != nil {
			log.Printf("Error processing %s: %v", result.File, result.Error)
		} else {
			log.Printf("Finished processing %s: processed=%d, errors=%d, execution time: %f sec",
				result.File, result.Processed, result.Errors, executionTime)
		}
	}

	endTime := time.Now()
	totalTime := endTime.Sub(startTime).Seconds()
	log.Printf("Total processed: %d, total errors: %d, total execution time: %f sec",
		totalProcessed, totalErrors, totalTime)
}
