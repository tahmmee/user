package couchbase

import (
	"flag"
	"fmt"
	"os"

	"github.com/couchbase/gocb"
	"github.com/microservices-demo/user/users"
)

var (
	name     string
	password string
	host     string
	bucket   = "default"
)

func init() {
	flag.StringVar(&host, "couchbase-host", os.Getenv("COUCHBASE_HOST"), "Couchbase host")
	flag.StringVar(&name, "couchbase-user", os.Getenv("COUCHBASE_USER"), "Couchbase user")
	flag.StringVar(&password, "couchbase-password", os.Getenv("COUCHBASE_PASSWORD"), "Couchbase password")
}

// Couchbase meets the Database interface requirements
type Couchbase struct {
	// cluster is a connection handle to couchbase
	Cluster *gocb.Cluster
	Bucket  *gocb.Bucket
}

func (c *Couchbase) Init() error {
	var err error
	clusterUrl := fmt.Sprintf("couchbase://%s", host)
	auth := gocb.PasswordAuthenticator{name, password}
	clusterOpts := gocb.ClusterOptions{
		Authenticator: auth,
	}
	c.Cluster, err = gocb.Connect(clusterUrl, clusterOpts)
	if err != nil {
		return fmt.Errorf("Connection error: %v", err)
	}

	c.Bucket = c.Cluster.Bucket(bucket, nil)
	if c.Bucket == nil {
		return fmt.Errorf("Open bucket error: %v", err)
	}

	return nil
}

func (c *Couchbase) GetUserByName(string) (users.User, error) {
	u := users.User{}
	return u, nil
}

func (c *Couchbase) GetUser(string) (users.User, error) {
	u := users.User{}
	return u, nil
}
func (c *Couchbase) GetUsers() ([]users.User, error) {
	us := []users.User{}
	statement := "select * from `default` where kind='customer'"
	res, err := c.Cluster.Query(statement, nil)
	if err != nil {
		return us, fmt.Errorf("failed to run customer query: %v", err)
	}
	val := make(map[string]users.User)
	for res.Next(&val) {
		us = append(us, val[bucket])
	}

	return us, nil
}

func (c *Couchbase) CreateUser(*users.User) error {
	return nil
}

func (c *Couchbase) GetUserAttributes(*users.User) error {
	return nil
}

func (c *Couchbase) GetAddress(string) (users.Address, error) {
	addr := users.Address{}
	return addr, nil
}

func (c *Couchbase) GetAddresses() ([]users.Address, error) {
	addrs := []users.Address{}
	return addrs, nil
}

func (c *Couchbase) CreateAddress(*users.Address, string) error {
	return nil
}

func (c *Couchbase) GetCard(string) (users.Card, error) {
	card := users.Card{}
	return card, nil
}

func (c *Couchbase) GetCards() ([]users.Card, error) {
	cards := []users.Card{}
	return cards, nil
}

func (c *Couchbase) Delete(string, string) error {
	return nil
}

func (c *Couchbase) CreateCard(*users.Card, string) error {
	return nil
}

func (c *Couchbase) Ping() error {
	return nil
}
