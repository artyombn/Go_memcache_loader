#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 1000  # for set_multi
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc, batch, dry_run=False):
    data = {}
    for appinst in batch:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appinst.lat
        ua.lon = appinst.lon
        key = "%s:%s" % (appinst.dev_type, appinst.dev_id)
        ua.apps.extend(appinst.apps)
        packed = ua.SerializeToString()
        data[key] = packed
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc.servers[0], key, packed))
            return True, len(batch)
        else:
            if not memc.set_multi(data):
                return False, len(batch)
            return True, len(batch)
    except Exception as e:
        logging.exception("Cannot write %s: %s" % (memc.servers[0], e))
        return False

def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isdigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def worker(file, memc_clients, dry_run):
    processed = errors = 0
    start_time = datetime.now()
    logging.info('Processing %s' % file)

    try:
        batch = {
            dev_type: [] for dev_type in memc_clients
        }

        with gzip.open(file, 'rt') as fd:
            for line in fd:
            # for i, line in enumerate(fd):
            #     if i >= 200:
            #         break
                line = line.strip()
                if not line:
                    continue
                appsinstalled = parse_appsinstalled(line)
                if not appsinstalled:
                    errors += 1
                    continue
                dev_type = appsinstalled.dev_type
                if dev_type not in memc_clients:
                    errors += 1
                    logging.error("Unknow device type: %s" % dev_type)
                    continue
                batch[dev_type].append(appsinstalled)

                if len(batch[dev_type]) >= BATCH_SIZE:
                    ok, count = insert_appsinstalled(memc_clients[dev_type], batch[dev_type], dry_run)
                    processed += count if ok else 0
                    errors += 0 if ok else count
                    batch[dev_type].clear()

        for dev_type, b in batch.items():
            if b:
                ok, count = insert_appsinstalled(memc_clients[dev_type], b, dry_run)
                processed += count if ok else 0
                errors += 0 if ok else count

    except Exception as e:
        logging.error(f"Error processing file {file}: {e}")

    if not processed:
        dot_rename(file)
    else:
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        dot_rename(file)

    finish_time = datetime.now()
    return file, processed, errors, start_time, finish_time


def main(options):
    memc_clients = {
        "idfa": memcache.Client([options.idfa]),
        "gaid": memcache.Client([options.gaid]),
        "adid": memcache.Client([options.adid]),
        "dvid": memcache.Client([options.dvid]),
    }

    files = sorted(glob.glob(options.pattern))
    if not files:
        logging.warning("No files found matching pattern %s" % options.pattern)
        return

    total_processed = total_errors = 0
    max_workers = len(files)
    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, file, memc_clients, options.dry): file for file in files}
        for future in futures:
            try:
                file, processed, errors, start_time, finish_time = future.result()
                total_processed += processed
                total_errors += errors
                logging.info(f"Finished processing {future}: "
                             f"processed={processed}, "
                             f"errors={errors}, "
                             f"execution time: {(finish_time - start_time).total_seconds()} sec")
            except Exception as e:
                logging.error(f"Error in processing {future}: {e}")

    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds()
    logging.info(f"Total processed: {total_processed}, total errors: {total_errors}, total execution time: {total_time} sec")

def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/logs/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
