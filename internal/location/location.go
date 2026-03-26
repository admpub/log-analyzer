package location

import (
	"net/netip"

	"github.com/oschwald/geoip2-golang/v2"
)

func GetCountryCode(ipAddress netip.Addr) (string, error) {
	db, err := geoip2.Open("internal/location/GeoLite2-Country.mmdb")
	if err != nil {
		return "", err
	}
	defer db.Close()

	record, err := db.Country(ipAddress)
	if err != nil {
		return "", err
	}
	location := record.Country.Names.English
	return location, nil
}
