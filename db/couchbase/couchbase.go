package couchbase

import (
	"flag"
	"fmt"
	"os"

	"github.com/couchbase/gocb"
	"github.com/microservices-demo/user/users"
	"gopkg.in/mgo.v2/bson"
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

// CouchbaseUser is a wrapper for the users
type CouchbaseUser struct {
	users.User `bson:",inline"`
	Kind       string          `bson:"kind"`
	ID         bson.ObjectId   `bson:"_id"`
	AddressIDs []bson.ObjectId `bson:"addresses"`
	CardIDs    []bson.ObjectId `bson:"cards"`
}

// New Returns a new CouchbaseUser
func New() CouchbaseUser {
	u := users.New()
	return CouchbaseUser{
		User:       u,
		AddressIDs: make([]bson.ObjectId, 0),
		CardIDs:    make([]bson.ObjectId, 0),
	}
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

func (c *Couchbase) GetUser(id string) (users.User, error) {
	u := users.User{}

	col := c.Bucket.DefaultCollection(nil)
	res, err := col.Get(id, nil)
	if err != nil {
		return u, err
	}

	if err := res.Content(&u); err != nil {
		return u, err
	}

	return u, nil
}

func (c *Couchbase) GetUsers() ([]users.User, error) {
	us := []users.User{}
	statement := "select  meta(`default`).id,`default`.*  from `default` where username is not missing"
	res, err := c.Cluster.Query(statement, nil)
	if err != nil {
		return us, fmt.Errorf("failed to get customers: %v", err)
	}
	u := users.User{}
	for res.Next(&u) {
		us = append(us, u)
	}

	return us, nil
}

func (c *Couchbase) CreateUser(u *users.User) error {

	// new couchbase user
	cu := New()
	cu.User = *u
	cu.ID = bson.NewObjectId()
	cu.Kind = "customer"

	// store
	col := c.Bucket.DefaultCollection(nil)
	_, err := col.Upsert(string(cu.ID), cu, nil)
	if err != nil {
		return err
	}

	cu.User.UserID = cu.ID.Hex()
	*u = cu.User

	return nil
}

// For login
func (c *Couchbase) GetUserAttributes(*users.User) error {
	return nil
}

func (c *Couchbase) GetAddress(id string) (users.Address, error) {
	addr := users.Address{}

	col := c.Bucket.DefaultCollection(nil)
	res, err := col.Get(id, nil)
	if err != nil {
		return addr, err
	}

	if err := res.Content(&addr); err != nil {
		return addr, err
	}

	return addr, nil
}

func (c *Couchbase) GetAddresses() ([]users.Address, error) {
	addrs := []users.Address{}

	statement := "select meta(`default`).id,`default`.* from `default` where postcode is not missing"
	res, err := c.Cluster.Query(statement, nil)
	if err != nil {
		return addrs, fmt.Errorf("failed to get addresses: %v", err)
	}

	a := users.Address{}
	for res.Next(&a) {
		addrs = append(addrs, a)
	}
	return addrs, nil
}

func (c *Couchbase) CreateAddress(*users.Address, string) error {
	return nil
}

func (c *Couchbase) GetCard(id string) (users.Card, error) {
	card := users.Card{}

	col := c.Bucket.DefaultCollection(nil)
	res, err := col.Get(id, nil)
	if err != nil {
		return card, err
	}

	if err := res.Content(&card); err != nil {
		return card, err
	}

	return card, nil
}

func (c *Couchbase) GetCards() ([]users.Card, error) {
	cards := []users.Card{}

	statement := "select meta(`default`).id,`default`.* from `default` where longNum is not missing"
	res, err := c.Cluster.Query(statement, nil)
	if err != nil {
		return cards, fmt.Errorf("failed to get cards: %v", err)
	}
	card := users.Card{}
	for res.Next(&card) {
		cards = append(cards, card)
	}

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
