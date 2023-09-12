package scraper

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type MenyScraper struct {
}

type FetchResult struct {
	Products    []Product
	PricePoints []PricePoint
	Vendors     []Vendor
}

type sourceRow struct {
	Title                string  `json:"title"`
	SubTitle             string  `json:"subtitle"`
	Vendor               string  `json:"vendor"`
	IsOffer              bool    `json:"isOffer"`
	ImageGtin            string  `json:"imageGtin"`
	PricePerUnit         float64 `json:"pricePerUnit"`
	PricePerUnitOriginal float64 `json:"pricePerUnitOriginal"`
	MeasurementValue     float64 `json:"measurementValue"`
	MeasureMentType      string  `json:"measurementType"`
	Unit                 string  `json:"unit"`
	StoreId              string  `json:"storeId"`
	CategoryName         string  `json:"categoryName"`
	SupplierId           int     `json:"supplierId"`
}

func (s *sourceRow) volume() (Volume, error) {
	return newQuantity((s.MeasurementValue), s.MeasureMentType)
}
func (s *sourceRow) id() string {
	return s.ImageGtin
}
func (s *sourceRow) product() (*Product, error) {
	q, err := s.volume()
	if err != nil {
		return nil, err
	}
	vendorId := strconv.Itoa(s.SupplierId)
	image := s.ImageGtin
	return &Product{
		ID:          s.id(),
		Name:        s.Title,
		Description: s.SubTitle,
		Qty:         q,
		VendorID:    &vendorId,
		ImageURL:    &image,
		Categories:  nil,
	}, nil
}
func (s *sourceRow) pricePoint() *PricePoint {
	return &PricePoint{
		ProductID: s.id(),
		RetailID:  "meny",
		StoreID:   &s.StoreId,
		Price:     s.PricePerUnit,
		Date:      time.Now().Truncate(24 * time.Hour),
		IsOffer:   s.IsOffer,
	}
}

type menyRequest struct {
	pagesize int
	parallel int
	rl       time.Duration
	pages    []int
	wg       sync.WaitGroup
	client   *http.Client
}

type response struct {
	rows []sourceRow
}

func call(client *http.Client, page, pagesize int) (*response, error) {
	u, err := url.Parse("https://platform-rest-prod.ngdata.no/api/products/1300/7080001150488?page=3&page_size=4&full_response=true&fieldset=maximal&facets=Category%2CAllergen&showNotForSale=false")
	if err != nil {
		panic(err)
	}
	q := u.Query()
	q.Set("page", strconv.Itoa(page))
	q.Set("page_size", strconv.Itoa(pagesize))
	u.RawQuery = q.Encode()
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// parse and insert into data
	type raw struct {
		Hits struct {
			Hits []struct {
				Source sourceRow `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	var data raw
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	var rows []sourceRow
	for _, r := range data.Hits.Hits {
		rows = append(rows, r.Source)
	}
	return &response{rows: rows}, nil
}

type option func(*menyRequest) *menyRequest

func rateLimit(d time.Duration) option {
	return func(m *menyRequest) *menyRequest {
		m.rl = d
		return m
	}
}
func parallel(parallel int) option {
	return func(m *menyRequest) *menyRequest {
		m.parallel = parallel
		return m
	}
}
func pageRange(start, end int) option {
	var ps []int
	for i := start; i <= end; i++ {
		ps = append(ps, i)
	}
	return pages(ps...)
}
func pageSize(size int) option {
	return func(m *menyRequest) *menyRequest {
		m.pagesize = size
		return m
	}
}
func pages(pages ...int) option {
	return func(m *menyRequest) *menyRequest {
		m.pages = pages
		return m
	}
}
func newMenyApiRequest(opts ...option) *menyRequest {
	o := &menyRequest{parallel: 1, pagesize: 5, pages: []int{1}}
	for _, opt := range opts {
		o = opt(o)
	}
	return o
}

type rowError struct {
	row sourceRow
	err error
}

func (re rowError) Error() string {
	return fmt.Sprintf("error: %v (row='%s' id='%s')", re.err, re.row.Title, re.row.id())
}
func (m *menyRequest) Execute() struct {
	products []Product
	prices   []PricePoint
} {
	wg := sync.WaitGroup{}
	httpClient := &http.Client{}
	var (
		products         = make(chan Product)
		pricePoints      = make(chan PricePoint)
		vendors          = make(chan Vendor)
		errors           = make(chan error)
		done             = make(chan int)
		productResult    = make(map[string]Product)
		pricePointResult = make(map[string]PricePoint)
		vendorResult     = make(map[string]Vendor)
	)

	// spawn collector routine
	var remaining = make(map[int]struct{})
	for _, page := range m.pages {
		remaining[page] = struct{}{}
	}

	wg.Add(1)
	go func() {
		log.Info().Msg("collector routine started")
		defer wg.Done()
		for {
			select {
			case p := <-products:
				if _, exists := productResult[p.ID]; exists {
					log.Warn().Str("id", p.ID).Str("name", p.Name).Msg("duplicate product")
				}
				productResult[p.ID] = p
			case pp := <-pricePoints:
				if _, exists := pricePointResult[pp.ProductID]; exists {
					log.Warn().Str("id", pp.ProductID).Msg("duplicate price point")
				}
				pricePointResult[pp.ProductID] = pp
			case v := <-vendors:
				// duplicate vendors is kinda expected, so we don't log it
				vendorResult[v.ID] = v
			case err := <-errors:
				if re, ok := err.(rowError); ok {
					log.Error().Err(re).Msg("")
				} else {
					log.Error().Err(err).Msg("")
				}
			case p := <-done:
				delete(remaining, p)
			}
			if len(remaining) == 0 {
				return
			}
		}
	}()

	c := make(chan int, m.parallel)
	rateLimit := make(chan struct{}, m.parallel)
	for i := 0; i < m.parallel; i++ {
		rateLimit <- struct{}{}
	}
	go func() {
		for _, page := range m.pages {
			<-rateLimit
			c <- page
		}
		close(c)
	}()
	for page := range c {
		wg.Add(1)
		go func(page int) {
			defer wg.Done()
			defer func() { rateLimit <- struct{}{} }()
			log.Info().Int("page", page).Int("pageSize", m.pagesize).Int("total", len(m.pages)).Msg("fetching page")
			res, err := call(httpClient, page, m.pagesize)
			if err != nil {
				errors <- rowError{err: err}
			} else {
				res.Read(products, pricePoints, vendors, errors)
			}
			time.Sleep(m.rl)
			done <- page
		}(page)
	}

	wg.Wait()
	log.Info().Msg("all routines done")
	var out []Product
	for _, p := range productResult {
		out = append(out, p)
	}
	var prices []PricePoint
	for _, pp := range pricePointResult {
		prices = append(prices, pp)
	}
	return struct {
		products []Product
		prices   []PricePoint
	}{
		products: out,
		prices:   prices,
	}
}
func (fr *response) Read(products chan<- Product, pricePoints chan<- PricePoint, vendors chan<- Vendor, errors chan<- error) {
	log.Debug().Int("rows", len(fr.rows)).Msg("reading rows")
	for _, row := range fr.rows {
		hit := row
		p, err := hit.product()
		if err != nil {
			errors <- rowError{row: hit, err: err}
			continue
		}
		log.Debug().Str("id", p.ID).Msgf("reading row: %s %v", p, err)
		products <- *p
		pricePoints <- *hit.pricePoint()
		vendors <- Vendor{
			ID:   strconv.Itoa(hit.SupplierId),
			Name: hit.Vendor,
		}
	}
}

func (m *MenyScraper) Fetch() (*FetchResult, error) {
	resp := newMenyApiRequest(
		parallel(4),
		pageSize(100),
		rateLimit(time.Millisecond*500),
		pageRange(1, 500),
	).Execute()
	for _, r := range resp.prices {
		fmt.Printf("%s\n", r.Json())
	}
    for _, p := range resp.products {
        fmt.Printf("%s\n", p.Json())
    }
	return &FetchResult{}, nil
}
