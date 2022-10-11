package main

import (
	"context"
	"net"
	"strconv"

	"github.com/hazelcast/hazelcast-go-client"
    // "github.com/hazelcast/hazelcast-go-client/nearcache"

)

func newClient() (hazelcast.Client, error) {
	config := hazelcast.Config{}

    // ec := nearcache.EvictionConfig{}
	// ec.SetPolicy(nearcache.EvictionPolicyLFU)
	// ec.SetSize(keyCount)
	// ncc := nearcache.Config{
	// 	Name:     "benchmark",
	// 	Eviction: ec,
	// }
	// ncc.SetInvalidateOnChange(true)
	// config.AddNearCache(ncc)

	//config.Logger.Level=logger.ErrorLevel
	config.Cluster.Name = clusterName
	config.Cluster.Network.SetAddresses(net.JoinHostPort(host, strconv.Itoa(port)))
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	return *client, err
}
