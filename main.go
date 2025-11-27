package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"distributedstore/router"
)

func main() {
	c := router.NewCluster(5).
		WithHTTPPort(8080).
		WithDataDir("./data")

	c.Open()
	defer c.Close()

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user-%d", i)
		value := fmt.Sprintf("value-%d", i)
		c.Put(key, value)
	}

	samples := []int{0, 1, 10, 123, 999}
	fmt.Println("\nReading sample keys:")
	for _, i := range samples {
		key := fmt.Sprintf("user-%d", i)
		val, found, _ := c.Get(key)
		if !found {
			fmt.Printf("Key %s not found\n", key)
			os.Exit(1)
		}
		fmt.Printf("  %s = %s\n", key, val)
	}

	c.Sync()

	fmt.Println("\nData has been written to ./data/node*/{wals,ssts}/")

	fmt.Printf("\nCluster running at %s\n", c.HTTPAddr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

}
