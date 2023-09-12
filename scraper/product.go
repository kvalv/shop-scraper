package scraper

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type (
	Unit     string
	Retailer struct {
		ID   string
		Name string
		URL  string
	}
	Store struct {
		ID         string
		RetailerID string
		Name       string
	}
	Vendor struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	Volume struct {
		Unit  Unit    `json:"unit"`
		Value float64 `json:"value"`
	}
	Category struct {
		Name   string    `json:"name"`
		Parent *Category `json:"parent"`
	}
	Product struct {
		ID          string     `json:"id"`
		Name        string     `json:"name"`
		Description string     `json:"description"`
		Qty         Volume     `json:"qty"`
		VendorID    *string    `json:"vendorId"`
		ImageURL    *string    `json:"imageUrl"`
		Categories  []Category `json:"categories"`
	}
	PricePoint struct {
		ProductID string    `json:"productId"`
		RetailID  string    `json:"retailId"`
		StoreID   *string   `json:"storeId"`
		Price     float64   `json:"price"`
		Date      time.Time `json:"date"`
		IsOffer   bool      `json:"isOffer"`
	}
)

func (p *Product) String() string {
	// trim description to 50
	short := p.Description
	if len(short) > 50 {
		short = short[:47] + "..."
	}
	return fmt.Sprintf("<Product '%s - %s' id=%s>", p.Name, short, p.ID)
}
func (p *Product) Json() string {
	bytes, _ := json.MarshalIndent(p, "", "  ")
	return string(bytes)
}
func (pp *PricePoint) String() string {
    return fmt.Sprintf("<PricePoint '%s' %f>", pp.ProductID, pp.Price)
}
func (pp *PricePoint) Json() string {
    bytes, _ := json.MarshalIndent(pp, "", "  ")
    return string(bytes)
}

var (
	UnitVolume Unit = "L"
	UnitWeight Unit = "kg"
	UnitAmount Unit = "pcs"
)

func newQuantity(value float64, unit string) (Volume, error) {
	// c, xi -- stupid ones/

	var (
		v float64 = value
		u Unit
	)
	switch strings.ToLower(unit) {
	case "kg":
		u = UnitWeight
	case "g":
		u = UnitWeight
		v = v / 1000
	case "hg":
		u = UnitWeight
		v = v / 100
	case "l":
		u = UnitVolume
	case "ml":
		u = UnitVolume
		v = v / 1000
	case "dl":
		u = UnitVolume
		v = v / 10
	case "stk":
		u = UnitAmount
	case "c":
		u = UnitAmount
	case "pcs":
		u = UnitAmount
	default:
		return Volume{}, fmt.Errorf("unknown unit: '%s'", unit)
	}

	return Volume{
		Unit:  u,
		Value: v,
	}, nil
}
