package geoip

import (
	"net"
	"testing"

	"github.com/admpub/pp"
	"github.com/stretchr/testify/assert"
)

func TestLookup(t *testing.T) {
	return
	dbFile := `/home/swh/下载/dbip-city-lite-2025-10.mmdb`
	db, err := New(dbFile)
	assert.NoError(t, err)
	ip := net.ParseIP(`1.1.1.1`)
	recv := map[string]interface{}{}
	err = db.Lookup(ip, &recv)
	assert.NoError(t, err)
	pp.Println(recv)

	var rec DBRecord
	rec, err = db.LookupCountry(ip)
	assert.NoError(t, err)
	pp.Println(rec)
}
