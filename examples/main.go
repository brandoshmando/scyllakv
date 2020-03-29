package main

import (
	"fmt"

	"github.com/brandoshmando/scyllakv"
)

func withHosts(hosts []string) scyllakv.Option {
	return func(c *scyllakv.Client) {
		c.Hosts = hosts
	}
}

func main() {
	opt := withHosts([]string{"localhost:9042"})
	c, err := scyllakv.New(opt)
	if err != nil {
		fmt.Println("Error creating client:", err)
		return
	}

	table, err := c.CreateTableIfNotExists("test", nil)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	// First get
	first, ok, err := table.Get([]byte("fakeKey"))
	if err != nil {
		fmt.Println("Error during initial get:", err)
		return
	}

	fmt.Printf("First Get: %v - %q\n", ok, first)

	key := []byte("706c197a-656f-465d-a927-3caabb8a9465")
	val := []byte("Hello World!")

	// First Put
	if err := table.Put(key, val); err != nil {
		fmt.Println("Error during initial put:", err)
		return
	}

	// Second get
	second, ok, err := table.Get(key)
	if err != nil {
		fmt.Println("Error during second get:", err)
		return
	}

	fmt.Printf("Second Get: %v - %q\n", ok, second)

	// Delete
	if err := table.Delete(key); err != nil {
		fmt.Println("Error during delete:", err)
		return
	}

	third, ok, err := table.Get(key)
	if err != nil {
		fmt.Println("Error during third get:", err)
		return
	}

	fmt.Printf("Third Get: %v - %q\n", ok, third)

}
