package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
)

var host = "10.212.40.101"

// "127.0.0.1"
var port = 5701
var keyCount = 1000
var valueSize = 128
var requests = 1_000_000
var clients = 10
var mapName = "benchmark"
var setRatio = 1
var getRatio = 10
var ratioStr = strconv.Itoa(setRatio) + ":" + strconv.Itoa(getRatio)
var clusterName = "dev"

func parseFlags() {
	flag.StringVar(&host, "h", host, "Server hostname")
	flag.IntVar(&port, "p", port, "Server port")
	flag.IntVar(&keyCount, "r", keyCount, "Key space range")
	flag.IntVar(&valueSize, "d", valueSize, "Data size in bytes")
	flag.IntVar(&requests, "n", requests, "Number of total requests")
	flag.IntVar(&clients, "c", clients, "Number of client threads")
	flag.StringVar(&mapName, "m", mapName, "Name of the Hazelcast IMap")
	flag.StringVar(&ratioStr, "ratio", ratioStr, "Set:Get ratio")
	flag.StringVar(&clusterName, "cluster", clusterName, "Hazelcast cluster name")

	flag.Parse()

	split := strings.Split(ratioStr, ":")
	if len(split) != 2 {
		exit(fmt.Errorf("wrong ratio parameter: %s\n", ratioStr))
	}

	r, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		exit(err)
	}
	setRatio = int(r)

	r, err = strconv.ParseInt(split[1], 10, 32)
	if err != nil {
		exit(err)
	}
	getRatio = int(r)
}
