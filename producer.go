package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
	"os/signal"
    "syscall"

	"github.com/redis/go-redis/v9"
)

type GraphQLResponse struct {
	Data struct {
		Products struct {
			PageInfo struct {
				HasNextPage bool   `json:"hasNextPage"`
				EndCursor   string `json:"endCursor"`
			} `json:"pageInfo"`
			Edges []struct {
				Node struct {
					ID            string `json:"id"`
					ProductStatus struct{ Value string } `json:"productStatus"`
					ImageStatus   struct{ Value string } `json:"imageStatus"`
					AIJson        struct{ Value string } `json:"aiJson"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"products"`
	} `json:"data"`
	Extensions struct {
		Cost struct {
			ActualQueryCost int `json:"actualQueryCost"`
			ThrottleStatus  struct {
				MaximumAvailable   float64 `json:"maximumAvailable"`
				CurrentlyAvailable float64 `json:"currentlyAvailable"`
				RestoreRate        float64 `json:"restoreRate"`
			} `json:"throttleStatus"`
		} `json:"cost"`
	} `json:"extensions"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type AIJson struct {
	ProcessTime time.Time `json:"processTime"`
}

type ProductData struct {
	ID            string
	ProductStatus []string
	ImageStatus   []string
	AIJsonRaw     string
}

var (
	rdb        *redis.Client
	shopifyURL = os.Getenv("SHOPIFY_SHOP_URL")
)

func main() {
	if shopifyURL == "" {
		shopifyURL = "lt-data.myshopify.com"
	}

	redisURL := os.Getenv("REDIS_URL")
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		rdb = redis.NewClient(&redis.Options{Addr: redisURL})
	} else {
		rdb = redis.NewClient(opt)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

	token, err := getAccessToken(shopifyURL)
	if err != nil {
		log.Fatalf("Could not get Shopify token: %v", err)
	}

	streamProductsAndQueue(ctx, token)
}

func streamProductsAndQueue(ctx context.Context, token string) {
	cursor := ""
	for {
		select {
        case <-ctx.Done():
            log.Println("Producer: Stopping product stream...")
            return
        default:
			query := fmt.Sprintf(`
			{
				products(first: 50, after: %s) {
					pageInfo { hasNextPage endCursor }
					edges {
						node {
							id
							productStatus: metafield(namespace: "custom", key: "product_status") { value }
							imageStatus: metafield(namespace: "custom", key: "image_status") { value }
							aiJson: metafield(namespace: "custom", key: "ai_json") { value }
						}
					}
				}
			}`, formatCursor(cursor))

			respData, err := makeShopifyRequest(ctx, query, token)
			if err != nil {
				log.Printf("Error fetching batch: %v", err)
				return
			}

			for _, edge := range respData.Data.Products.Edges {
				pkg := ProductData{
					ID:            edge.Node.ID,
					ProductStatus: parseList(edge.Node.ProductStatus.Value),
					ImageStatus:   parseList(edge.Node.ImageStatus.Value),
					AIJsonRaw:     edge.Node.AIJson.Value,
				}

				if compareDates(pkg) {
					clearExistingErrors(ctx, rdb, pkg.ID)
					err := rdb.LPush(ctx, "ai_queue", pkg.ID).Err()
					if err != nil {
						log.Printf("Failed to queue product %s: %v", pkg.ID, err)
					} else {
						fmt.Printf("Queued: %s\n", pkg.ID)
					}
				}
			}

			handleRateLimit(respData)

			if !respData.Data.Products.PageInfo.HasNextPage {
				log.Println("Producer: Finished scanning all products. Task complete.")
				return
			}
			cursor = respData.Data.Products.PageInfo.EndCursor
		}
	}
}

func handleRateLimit(resp *GraphQLResponse) {
	if resp == nil { return }
	
	stats := resp.Extensions.Cost.ThrottleStatus
	if stats.CurrentlyAvailable < (stats.MaximumAvailable * 0.2) {
		waitSec := (100.0 / stats.RestoreRate)
		fmt.Printf("Rate limit low (%.1f/%.1f). Sleeping %.2fs...\n", stats.CurrentlyAvailable, stats.MaximumAvailable, waitSec)
		time.Sleep(time.Duration(waitSec * float64(time.Second)))
	}
}

func compareDates(p ProductData) bool {
	if p.AIJsonRaw == "" || p.AIJsonRaw == "null" {
		return true
	}

	var aiData AIJson
	err := json.Unmarshal([]byte(p.AIJsonRaw), &aiData)
	if err != nil {
		return true
	}

	latestStatus := findLatestDate(append(p.ProductStatus, p.ImageStatus...))

	return latestStatus.After(aiData.ProcessTime)
}
func findLatestDate(dateStrings []string) time.Time {
	var latest time.Time
	layouts := []string{time.RFC3339, "2006-01-02T15:04:05Z07:00", "2006-01-02"}

	found := false
	for _, s := range dateStrings {
		if s == "" || s == "[]" { continue }
		for _, layout := range layouts {
			t, err := time.Parse(layout, s)
			if err == nil {
				if !found || t.After(latest) {
					latest = t
					found = true
				}
				break
			}
		}
	}

	if !found {
		return time.Now()
	}
	return latest
}
func formatCursor(cursor string) string {
	if cursor == "" {
		return "null"
	}
	return fmt.Sprintf("%q", cursor)
}

func parseList(jsonStr string) []string {
	list := []string{}
	if jsonStr == "" || jsonStr == "null" {
		return list
	}
	err := json.Unmarshal([]byte(jsonStr), &list)
	if err != nil {
		log.Printf("Warning: failed to parse metafield list: %v", err)
		return []string{}
	}

	return list
}

func makeShopifyRequest(ctx context.Context, query string, token string) (*GraphQLResponse, error) {
	apiVersion := os.Getenv("SHOPIFY_API_VERSION")
	if apiVersion == "" { apiVersion = "2026-01" }

	jsonData := map[string]string{"query": query}
	body, _ := json.Marshal(jsonData)

	endpoint := fmt.Sprintf("https://%s/admin/api/%s/graphql.json", shopifyURL, apiVersion)
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(body))
	if err != nil { return nil, err }

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Shopify-Access-Token", token)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil { return nil, err }
	defer resp.Body.Close()

	var result GraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	
	if len(result.Errors) > 0 {
		return &result, fmt.Errorf("shopify error: %s", result.Errors[0].Message)
	}

	return &result, nil
}

func getAccessToken(shopURL string) (string, error) {
	clientID := os.Getenv("SHOPIFY_CLIENT_ID")
	clientSecret := os.Getenv("SHOPIFY_CLIENT_SECRET")

	url := fmt.Sprintf("https://%s/admin/oauth/access_token", shopURL)
	payload := map[string]string{
		"client_id":     clientID,
		"client_secret": clientSecret,
		"grant_type":    "client_credentials",
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil { return "", err }
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	
	if token, ok := result["access_token"].(string); ok {
		return token, nil
	}
	return "", fmt.Errorf("access_token not found in response")
}

func clearExistingErrors(ctx context.Context, rdb *redis.Client, id string) {
	rdb.HDel(ctx, "product_errors", id)
}
