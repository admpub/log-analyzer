package geoip

import (
	"net/netip"
	"testing"

	"github.com/admpub/pp"
	"github.com/stretchr/testify/assert"
)

func TestLookup(t *testing.T) {
	t.Skip()
	dbFile := `/home/swh/下载/dbip-city-lite-2025-10.mmdb`
	db, err := New(dbFile)
	assert.NoError(t, err)
	ip, err := netip.ParseAddr(`1.1.1.1`)
	assert.NoError(t, err)
	recv := map[string]interface{}{}
	err = db.Lookup(ip, &recv)
	assert.NoError(t, err)
	pp.Println(recv)

	var rec DBRecord
	rec, err = db.LookupCountry(ip)
	assert.NoError(t, err)
	pp.Println(rec)
}
