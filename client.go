package scyllakv

import (
	"fmt"

	"github.com/gocql/gocql"
)

// Table represents a table within a scylladb keyspace.
type Table struct {
	client   *Client
	keyspace string
	name     string
}

// Get takes a byte key and querie the scylladb table for a value with the given
// key. If the returned value was was found, the returned bool will be true.
func (t *Table) Get(k []byte) ([]byte, bool, error) {
	var key, value []byte
	err := t.client.Query(fmt.Sprintf("SELECT value FROM %v.%v WHERE key = ?", t.keyspace, t.name), k).Scan(&key, &value)

	return value, len(key) > 0, err
}

// Put uses the cql UPDATE query to upsert a given byte value within a table using
// the given byte key.
func (t *Table) Put(k, v []byte) error {
	return t.client.Query(fmt.Sprintf("UPDATE %v.%v SET value = ? WHERE key = ?", t.keyspace, t.name), v, k).Exec()
}

// Delete removes a key / value pair from a table using the given byte key
func (t *Table) Delete(k []byte) error {
	return t.client.Query(fmt.Sprintf("DELETE FROM %v.%v WHERE key = ?", t.keyspace, t.name)).Exec()
}

// Client provides necessary components for communicating with a scylladb instance
type Client struct {
	*gocql.Session
	*gocql.ClusterConfig
}

// helper function for creating a keyspace if it does not exist
func (c *Client) createKeyspaceIfNotExists(k string) error {
	// TODO: make options in with block configurable
	return c.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %v WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", k)).Exec()
}

// helper function for creating a table if it does not exist
func (c *Client) createTableIfNotExists(k, n string) error {
	return c.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v.%v (key blob PRIMARY KEY, value blob)};", k, n)).Exec()
}

// CreateTableIfNotExists will create a table with the given name within the given
// keyspace. The table that is created is a simple two column, key value store
// where the key is the primary key and the value is the corresponding value. The
// underlying data types are arbitrary bytes with no validation.
//
// If the keyspace does not already exist, it also will be created. If
// the given keyspace is nil, the table will be created within a keyspace named
// 'default'.
func (c *Client) CreateTableIfNotExists(name string, keyspace *string) (*Table, error) {
	var ks string
	if keyspace == nil {
		ks = "default"
	} else {
		ks = *keyspace
	}

	if err := c.createKeyspaceIfNotExists(ks); err != nil {
		return nil, err
	}

	if err := c.createTableIfNotExists(ks, name); err != nil {
		return nil, err
	}

	return &Table{
		client:   c,
		keyspace: ks,
		name:     name,
	}, nil
}

// Option is a function that can be used to customize the underlying gocql.ClusterConfig
type Option func(*gocql.ClusterConfig)

// New accepts many Option functions that will be called on the subsequent gocql.ClusterConfig.
// At minimum, an option that sets the Hosts attr of the gocql.ClusterConfig must
// be passed into the function. If a gocql.Session is created successfully, a new
// Client will be returned.
func New(opts ...Option) (*Client, error) {
	c := gocql.NewCluster()
	for _, opt := range opts {
		opt(c)
	}

	s, err := c.CreateSession()
	if err != nil {
		return nil, err
	}

	return &Client{s, c}, nil
}