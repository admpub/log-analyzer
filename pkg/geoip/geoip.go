package geoip

import (
	"net"

	"github.com/admpub/log"
	"github.com/oschwald/maxminddb-golang"
)

type Names struct {
	En string `maxminddb:"en"`
	//ZhCN string `maxminddb:"zh-CN"`
}

type DBRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
		Names   Names  `maxminddb:"names"` // {en: "China"}
	} `maxminddb:"country"`
	Location struct {
		Longitude float64 `maxminddb:"longitude"`
		Latitude  float64 `maxminddb:"latitude"`
	} `maxminddb:"location"`
	Subdivisions []struct {
		Names Names `maxminddb:"names"` // {en: "Sichuan"}
	} `maxminddb:"subdivisions"`
	City struct {
		Names Names `maxminddb:"names"` // {en: "Chengdu"}
	} `maxminddb:"city"`
	Continent struct { // 州
		Code  string `maxminddb:"code"`  // AS
		Names Names  `maxminddb:"names"` // {en: "Asia"}
	} `maxminddb:"continent"`
}

// No. D, C Road, B Town, A District, Shenzhen City, Guangdong Province.
func (d DBRecord) LocationString() string {
	var lo string
	var sep string
	if len(d.Continent.Names.En) > 0 {
		lo = d.Continent.Names.En + sep + lo
		sep = `, `
	}
	lo = d.Country.Names.En + sep + lo
	sep = `, `
	if len(d.Subdivisions) > 0 {
		for _, row := range d.Subdivisions {
			if len(row.Names.En) == 0 {
				continue
			}
			lo = row.Names.En + sep + lo
		}
	}
	if len(d.City.Names.En) > 0 {
		lo = d.City.Names.En + sep + lo
	}
	return lo
}

// DB is a GeoIP database provider
type DB struct {
	reader *maxminddb.Reader
}

// New GeoIP database provider
func New(filename string) (*DB, error) {
	if filename == "" {
		return nil, nil
	}
	reader, err := maxminddb.Open(filename)
	if err != nil {
		return nil, err
	}
	log.Debugf("using geo IP database: %s", filename)
	return &DB{
		reader: reader,
	}, nil
}

// Close GeoIP database
func (db *DB) Close() error {
	if db.reader != nil {
		return db.reader.Close()
	}
	return nil
}

// LookupCountry find IP
func (db *DB) Lookup(ip net.IP, recv interface{}) error {
	err := db.reader.Lookup(ip, recv)
	return err
}

// LookupCountry find IP country code
func (db *DB) LookupCountry(ip net.IP) (DBRecord, error) {
	record := DBRecord{}
	err := db.reader.Lookup(ip, &record)
	if err != nil {
		return record, err
	}
	return record, nil
}
