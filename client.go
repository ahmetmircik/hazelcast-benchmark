package main

import (
	"context"
	"net"
	"strconv"

	"github.com/hazelcast/hazelcast-go-client"
)

func newClient(ctx context.Context) (*hazelcast.Client, error) {
	config := hazelcast.NewConfig()
	config.Logger.Level = "error"
	config.Cluster.Name = clusterName
	networkConfig := config.Cluster.Network
	networkConfig.SetAddresses(net.JoinHostPort(host, strconv.Itoa(port)))
	return hazelcast.StartNewClientWithConfig(ctx, config)
}
