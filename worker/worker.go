package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"sync"
	"os/signal"
    "syscall"
	"math/rand"

	"github.com/redis/go-redis/v9"
)

type ShopifyProductDetails struct {
	ID            string   `json:"id"`
	ProductType   string   `json:"productType"`
	Vendor        string   `json:"vendor"`
	Sku           string   `json:"sku"`
	Status        string   `json:"status"`
	Tags          []string `json:"tags"`
	SapTitle      struct{ Value string } `json:"sapTitle"`
	Occasion struct {
		Reference struct {
			Field struct {
				Value string `json:"value"`
			} `json:"field"`
		} `json:"reference"`
	} `json:"occasion"`
	Media         struct {
		Edges []struct {
			Node struct {
				ID    string `json:"id"`
				Image struct {
					URL string `json:"url"`
				} `json:"image"`
			} `json:"node"`
		} `json:"edges"`
	} `json:"media"`
	Variants struct {
		Edges []struct {
			Node struct {
				Sku string `json:"sku"`
			} `json:"node"`
		} `json:"edges"`
	} `json:"variants"`
	VariantStatus struct{ Value string } `json:"variantStatus"`
	ProductStatus struct{ Value string } `json:"productStatus"`
	ImageStatus   struct{ Value string } `json:"imageStatus"`
	AiStatus      struct{ Value string } `json:"aiStatus"`
	AiStatusRaw   string
	ToneOptions   struct{ Validations []struct{ Name, Value string } `json:"validations"` } `json:"toneOptions"`
	GroupOptions  struct{ Validations []struct{ Name, Value string } `json:"validations"` } `json:"groupOptions"`
	GenderOptions struct{ Validations []struct{ Name, Value string } `json:"validations"` } `json:"genderOptions"`
}

type GraphQLProductResponse struct {
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
	Data struct {
		ProductUpdate struct {
			UserErrors []struct {
				Field   []string `json:"field"`
				Message string   `json:"message"`
			} `json:"userErrors"`
		} `json:"productUpdate"`
		FileUpdate struct {
			UserErrors []struct {
				Message string `json:"message"`
			} `json:"userErrors"`
		} `json:"fileUpdate"`
		Shop struct {
			ID            string `json:"id"`
			LogMetafield struct {
				Value string `json:"value"`
			} `json:"logMetafield"`
		} `json:"shop"`
		Product ShopifyProductDetails `json:"product"`
		MetafieldDefinitions struct {
			Edges []struct {
				Node struct {
					Key              string `json:"key"`
					Validations []struct{ Name, Value string } `json:"validations"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"metafieldDefinitions"`
	} `json:"data"`
}

type GeminiResponse struct {
	RecipientGender string   `json:"RecipientGender"`
	RecipientGroup  string   `json:"RecipientGroup"`
	RecipientKid    bool     `json:"RecipientKid"`
	Tone            string   `json:"Tone"`
	Description     string   `json:"Description"`
	Title           string   `json:"Title"`
	MetaDescription string   `json:"MetaDescription"`
	Keywords        []string `json:"Keywords"`
	AltText         []struct {
		ID  string `json:"id"`
		Alt string `json:"alt"`
	} `json:"altText"`
	RatingLanguage  int `json:"RatingLanguage"`
	RatingSexual    int `json:"RatingSexual"`
	RatingPolitical int `json:"RatingPolitical"`
	RatingNudity    int `json:"RatingNudity"`
}

var (
	rdb        *redis.Client
	shopifyURL = "lt-data.myshopify.com"
	accessToken string
	tokenExpiry time.Time
	tokenLock   sync.RWMutex
	model = "gemini-2.5-flash"
)

func main() {
	redisURL := os.Getenv("REDIS_URL")
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		rdb = redis.NewClient(&redis.Options{Addr: redisURL})
	} else {
		rdb = redis.NewClient(opt)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

	go startLogListener(ctx)

	const workerCount = 5
	limiter := time.NewTicker(2 * time.Second) 
    defer limiter.Stop()

	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for {
                select {
                case <-ctx.Done():
                    log.Printf("Worker %d: Shutting down gracefully...", workerID)
                    return
                default:
					<-limiter.C
                    result, err := rdb.BRPop(ctx, 5*time.Second, "ai_queue").Result()
                    if err != nil {
                        continue
                    }
                    
                    productID := result[1]
					log.Printf("Worker %d: Beginning process for Product ID: %s", workerID, productID)
                    _, err = processTask(ctx, productID)
                    if err != nil {
                        logToRedis(ctx, "error_logs", "AI", productID, "Process Failed", err.Error())
                    } else {
                        logToRedis(ctx, "info_logs", "AI", productID, "Process Succeeded", "Updated Shopify")
                    }
                }
            }
        }(i)
    }

	<-ctx.Done()
    log.Println("Shutdown signal received. Waiting for active tasks to complete...")
    wg.Wait()
    log.Println("All workers stopped. Exiting.")
}

func processTask(ctx context.Context, id string) (string, error) {
	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
	token, err := getValidToken(ctx, shopifyURL) 
	if err != nil {
		return id, fmt.Errorf("token refresh failed: %v", err)
	}

	details, err := fetchDetailedProduct(ctx, id, token)
	if err != nil {
		return id, err
	}

	aiData, err := callGemini(ctx, *details)
	if err != nil {
		return details.Sku, err
	}

	err = updateShopifyCore(ctx, id, *details, aiData, token)
	if err != nil {
		return details.Sku, err
	}

	if len(aiData.AltText) > 0 {
		updateImageAltTexts(ctx, aiData.AltText, token)
	}

	return details.Sku, nil
}

func callGemini(ctx context.Context, d ShopifyProductDetails) (*GeminiResponse, error) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", model, apiKey)

	extractNames := func(options []struct{ Name, Value string }) []string {
		var names []string
		for _, o := range options {
			names = append(names, o.Name)
		}
		return names
	}

	genders := extractNames(d.GenderOptions.Validations)
	groups := extractNames(d.GroupOptions.Validations)
	tones := extractNames(d.ToneOptions.Validations)

	var imageIDs []string
	for _, edge := range d.Media.Edges {
		imageIDs = append(imageIDs, edge.Node.ID)
	}

	occasionName := d.Occasion.Reference.Field.Value
	if occasionName == "" {
		occasionName = "general"
	}

	log.Printf("[DEBUG] GENDERS: %v", genders)
	log.Printf("[DEBUG] GROUP: %v", groups)
	log.Printf("[DEBUG] TONES: %v", tones)

	promptText := fmt.Sprintf(`
		ACT AS AN ECOMMERCE SEO EXPERT.
		Product: %s (Type: %s, Occasion: %s).

		STRICT CONSTRAINTS:
		1. RECIPIENT GENDER: Pick EXACTLY one from this list: %v
		2. RECIPIENT GROUP: Pick EXACTLY one from this list: %v
		3. TONE: Pick EXACTLY one from this list: %v
		4. TITLE: Create a punny/clever title (2-4 words). DO NOT use the words: "%s", "%s", or any chosen Tone/Gender/Group names.
		5. ALT TEXT: Provide an SEO-optimized alt description for EVERY Image ID provided: %v.
		6. DESCRIPTION: Professional ecommerce HTML description.
		7. KEYWORDS: Exactly 249 comma-separated keywords.

		Return ONLY valid JSON:
		{
			"RecipientGender": "string",
			"RecipientGroup": "string",
			"RecipientKid": bool,
			"Tone": "string",
			"Description": "string",
			"Title": "string",
			"MetaDescription": "string",
			"Keywords": ["string"],
			"altText": [{"id": "string", "alt": "string"}],
			"RatingLanguage": 1-5,
			"RatingSexual": 1-5,
			"RatingPolitical": 1-5,
			"RatingNudity": 1-5
		}
	`, d.SapTitle.Value, d.ProductType, occasionName, genders, groups, tones, occasionName, d.SapTitle.Value, imageIDs)

	parts := []map[string]interface{}{
		{"text": promptText},
	}

	for _, edge := range d.Media.Edges {
		if edge.Node.Image.URL != "" {
			b64Data, err := downloadAndBase64(ctx, edge.Node.Image.URL)
			if err == nil {
				parts = append(parts, map[string]interface{}{
					"inline_data": map[string]string{
						"mime_type": "image/jpeg",
						"data":      b64Data,
					},
				})
			}
		}
	}

	reqBody := map[string]interface{}{
		"contents": []map[string]interface{}{
			{"parts": parts},
		},
		"generationConfig": map[string]interface{}{
			"response_mime_type": "application/json",
		},
		"safetySettings": []map[string]interface{}{
			{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
			{"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
			{"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
			{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
			{"category": "HARM_CATEGORY_CIVIC_INTEGRITY", "threshold": "BLOCK_NONE"},
		},
	}
	
	jsonData, _ := json.Marshal(reqBody)
	var resp *http.Response
	err := withRetry(ctx, func() error {
		var err error
		resp, err = http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		return err
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var geminiRaw struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
			FinishReason  string `json:"finishReason"`
			SafetyRatings []struct {
				Category    string `json:"category"`
				Probability string `json:"probability"`
			} `json:"safetyRatings"`
		} `json:"candidates"`
		PromptFeedback struct {
			BlockReason string `json:"blockReason"`
		} `json:"promptFeedback"`
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&geminiRaw); err != nil {
		return nil, err
	}

	if geminiRaw.Error.Code != 0 {
		return nil, fmt.Errorf("Gemini API Error: %s (Status: %s)", geminiRaw.Error.Message, geminiRaw.Error.Status)
	}

	if len(geminiRaw.Candidates) == 0 {
		return nil, fmt.Errorf("empty response from Gemini (Prompt Blocked: %s)", geminiRaw.PromptFeedback.BlockReason)
	}

	candidate := geminiRaw.Candidates[0]
	if len(candidate.Content.Parts) == 0 {
		return nil, fmt.Errorf("empty response from Gemini (FinishReason: %s)", candidate.FinishReason)
	}

	var result GeminiResponse
	err = json.Unmarshal([]byte(geminiRaw.Candidates[0].Content.Parts[0].Text), &result)

	debugJSON, _ := json.Marshal(result)
	log.Printf("[DEBUG] AI BODY: %s", string(debugJSON))

	return &result, err
}

func updateShopifyCore(ctx context.Context, id string, productData ShopifyProductDetails, ai *GeminiResponse, token string) error {
	mutation := `
	mutation productUpdate($input: ProductInput!) {
		productUpdate(input: $input) {
			userErrors { field message }
		}
	}`

	kid := ""
	if ai.RecipientKid {
		kid = "kid"
	}

	occasionName := productData.Occasion.Reference.Field.Value
	if occasionName == "" {
		occasionName = "general" // Fallback if metaobject or name field is empty
	}

	raw_handle := fmt.Sprintf("%s-%s-%s-%s-for%s%s%s-%s", productData.Vendor, productData.ProductType, occasionName, ai.Tone, ai.RecipientGender, ai.RecipientGroup, kid, productData.Sku)

	seo_title := fmt.Sprintf("%s | %s %s %s", ai.Title, ai.Tone, occasionName, productData.ProductType)

	var finalTags []string
	for _, tag := range productData.Tags {
		if strings.Contains(strings.ToLower(tag), strings.ToLower(productData.Sku)) {
			finalTags = append(finalTags, tag)
		}
	}

	maxTotal := 250
	currentCount := len(finalTags)
	remainingBudget := maxTotal - currentCount

	if remainingBudget > 0 {
		if len(ai.Keywords) > remainingBudget {
			ai.Keywords = ai.Keywords[:remainingBudget]
		}
    	finalTags = append(finalTags, ai.Keywords...)
	} else {
		if len(finalTags) > maxTotal {
			finalTags = finalTags[:maxTotal]
		}
	}

	currentTime := time.Now().Format(time.RFC3339)
	var statusHistory []string
	if productData.AiStatusRaw != "" {
		json.Unmarshal([]byte(productData.AiStatusRaw), &statusHistory)
	}
	statusHistory = append(statusHistory, currentTime)
	newStatusValue, _ := json.Marshal(statusHistory)

	newStatus := productData.Status
	if productData.Status == "DRAFT" {
		if productData.ImageStatus.Value != "" && 
		   productData.ProductStatus.Value != "" && 
		   productData.VariantStatus.Value != "" {
			newStatus = "ACTIVE"
		}
	}

	fullAiData := map[string]interface{}{
		"response":    ai,
		"processTime": currentTime,
		"model":       model,
	}
	fullAiJSON, _ := json.Marshal(fullAiData)

	input := map[string]interface{}{
		"id": id,
		"title": ai.Title,
		"handle": slugify(raw_handle),
		"descriptionHtml": ai.Description,
		"tags": finalTags,
		"status": newStatus,
		"seo": map[string]interface{}{
			"description": ai.MetaDescription,
			"title": seo_title,
		},
		"metafields": []map[string]string{
			{"namespace": "custom", "key": "tone", "value": ai.Tone},
			{"namespace": "custom", "key": "recipient_child", "value": fmt.Sprintf("%v", ai.RecipientKid)},
			{"namespace": "custom", "key": "recipient_gender", "value": ai.RecipientGender},
			{"namespace": "custom", "key": "recipient_group", "value": ai.RecipientGroup},
			{"namespace": "custom", "key": "rating_language", "value": fmt.Sprintf("%d", ai.RatingLanguage)},
			{"namespace": "custom", "key": "rating_sexual", "value": fmt.Sprintf("%d", ai.RatingSexual)},
			{"namespace": "custom", "key": "rating_nudity", "value": fmt.Sprintf("%d", ai.RatingNudity)},
			{"namespace": "custom", "key": "rating_political", "value": fmt.Sprintf("%d", ai.RatingPolitical)},
			{"namespace": "custom", "key": "ai_json", "value": string(fullAiJSON)},
			{"namespace": "custom", "key": "ai_status", "value": string(newStatusValue)},
		},
	}

	_, err := sendGraphQL(ctx, mutation, map[string]interface{}{"input": input}, token)
	if err != nil {
		return err
	}

	return nil
}

func updateImageAltTexts(ctx context.Context, images []struct {
    ID  string `json:"id"`
    Alt string `json:"alt"`
}, token string) error {
	if len(images) == 0 { return nil }
	
	mutation := `
	mutation fileUpdate($files: [FileUpdateInput!]!) {
		fileUpdate(files: $files) {
			userErrors { message }
		}
	}`

	var files []map[string]string
	for _, a := range images {
		files = append(files, map[string]string{"id": a.ID, "alt": a.Alt})
	}


	_, err := sendGraphQL(ctx, mutation, map[string]interface{}{"files": files}, token)
	if err != nil {
		return err
	}

	return nil
}

func fetchDetailedProduct(ctx context.Context, id string, token string) (*ShopifyProductDetails, error) {
	combinedQuery := `
	query($id: ID!) {
		product(id: $id) {
			id
			productType
			vendor
			status
			tags
			variants(first: 1) { edges { node { sku } } }
			sapTitle: metafield(namespace: "custom", key: "sapTitle") { value }
			occasion: metafield(namespace: "custom", key: "occasion") { 
				reference {
					... on Metaobject {
						field(key: "name") { value }
					}
				}
			}    
			media(first: 50) { edges { node { id ... on MediaImage { image { url } } } } }
			variantStatus: metafield(namespace: "custom", key: "variant_status") { value }
			productStatus: metafield(namespace: "custom", key: "product_status") { value }
			imageStatus: metafield(namespace: "custom", key: "image_status") { value }
			aiStatus: metafield(namespace: "custom", key: "ai_status") { value }
		}
		metafieldDefinitions(ownerType: PRODUCT, first: 50) {
			edges {
				node {
					key
					validations { name value }
				}
			}
		}
	}`

	resp, err := sendGraphQL(ctx, combinedQuery, map[string]interface{}{"id": id}, token)
	if err != nil {
		return nil, fmt.Errorf("combined query failed: %w", err)
	}

	product := &resp.Data.Product

	for _, edge := range resp.Data.MetafieldDefinitions.Edges {
		switch edge.Node.Key {
		case "tone":
			product.ToneOptions.Validations = edge.Node.Validations
		case "recipient_group":
			product.GroupOptions.Validations = edge.Node.Validations
		case "recipient_gender":
			product.GenderOptions.Validations = edge.Node.Validations
		}
	}

	if len(product.Variants.Edges) > 0 {
		product.Sku = strings.Split(product.Variants.Edges[0].Node.Sku, "-")[0]
	}

	return product, nil
}

func sendGraphQL(ctx context.Context, query string, vars map[string]interface{}, token string) (*GraphQLProductResponse, error) {
	body, _ := json.Marshal(map[string]interface{}{"query": query, "variables": vars})
	var resp *http.Response
	err := withRetry(ctx, func() error {
		req, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://%s/admin/api/2026-01/graphql.json", shopifyURL), bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Shopify-Access-Token", token)

		client := &http.Client{Timeout: 20 * time.Second}
		var err error
		resp, err = client.Do(req)
		return err
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
        tokenLock.Lock()
        accessToken = ""
        tokenLock.Unlock()
        return nil, fmt.Errorf("unauthorized: token cleared")
    }

	if resp.StatusCode == 429 {
        log.Println("Shopify Rate Limit reached. Sleeping...")
        time.Sleep(5 * time.Second)
        return nil, fmt.Errorf("rate_limit_exceeded")
    }

	var res GraphQLProductResponse
	bodyBytes, _ := io.ReadAll(resp.Body)

	if err := json.Unmarshal(bodyBytes, &res); err != nil {
        return nil, err
    }

	if len(res.Errors) > 0 {
		return &res, fmt.Errorf("shopify graphql error: %s", res.Errors[0].Message)
	}
	
	if len(res.Data.ProductUpdate.UserErrors) > 0 {
        return &res, fmt.Errorf("shopify mutation error: %s", res.Data.ProductUpdate.UserErrors[0].Message)
    }

	if len(res.Data.FileUpdate.UserErrors) > 0 {
        return &res, fmt.Errorf("shopify file error: %s", res.Data.FileUpdate.UserErrors[0].Message)
    }

	return &res, nil
}

func startLogListener(ctx context.Context,) {
	for {
		token, err := getValidToken(ctx, shopifyURL) 
		if err != nil {
			log.Printf("Log listener token error: %v", err)
			time.Sleep(10 * time.Second)
			continue
		}

		result, err := rdb.BLPop(ctx, 0, "info_logs", "error_logs").Result()
		if err != nil {
			log.Printf("Redis Log Error: %v", err)
			continue
		}

		queueName := result[0]
		logMessage := result[1]

		log.Printf("Log Listener: Processing entry from %s: %s", queueName, logMessage)

		metafieldKey := "log_info"
		if queueName == "error_logs" {
			metafieldKey = "log_error"

			parts := strings.SplitN(logMessage, "|", 2)
			if len(parts) == 2 {
				feed := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(parts[0], "Feed", "")))
				details := strings.SplitN(parts[1], ":", 2)
				if len(details) == 2 {
					id := strings.TrimSpace(details[0])
					errDetail := strings.TrimSpace(details[1])

					errorObj := map[string]string{
						"id":    id,
						"feed":  feed,
						"error": errDetail,
					}

					payload, _ := json.Marshal(errorObj)

					rdb.HSet(ctx, "product_errors", id, payload)					
				}
			}
		}

		err = appendShopLog(ctx, metafieldKey, logMessage, token)
		if err != nil {
			log.Printf("Failed to update Shopify log: %v", err)
		}
	}
}

func getValidToken(ctx context.Context, shopURL string) (string, error) {
	tokenLock.RLock()
	if accessToken != "" && time.Now().Before(tokenExpiry.Add(-5*time.Minute)) {
		defer tokenLock.RUnlock()
		return accessToken, nil
	}
	tokenLock.RUnlock()

	tokenLock.Lock()
	defer tokenLock.Unlock()

	if accessToken != "" && time.Now().Before(tokenExpiry.Add(-5*time.Minute)) {
		return accessToken, nil
	}

	newToken, err := fetchNewTokenFromShopify(ctx, shopURL)
	if err != nil {
		return "", fmt.Errorf("failed to refresh shopify token: %w", err)
	}

	accessToken = newToken
	tokenExpiry = time.Now().Add(24 * time.Hour) 
	
	return accessToken, nil
}

func fetchNewTokenFromShopify(ctx context.Context, shopURL string) (string, error) {
	clientID := os.Getenv("SHOPIFY_CLIENT_ID")
	clientSecret := os.Getenv("SHOPIFY_CLIENT_SECRET")

	url := fmt.Sprintf("https://%s/admin/oauth/access_token", shopURL)
	payload := map[string]string{
		"client_id":     clientID,
		"client_secret": clientSecret,
		"grant_type":    "client_credentials",
	}

	body, _ := json.Marshal(payload)
	var resp *http.Response
	err := withRetry(ctx, func() error {
		var err error
		resp, err = http.Post(url, "application/json", bytes.NewBuffer(body))
		return err
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("shopify auth error: status %d", resp.StatusCode)
	}

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	
	if result.AccessToken == "" {
		return "", fmt.Errorf("access_token not found in response")
	}
	return result.AccessToken, nil
}

func downloadAndBase64(ctx context.Context, url string) (string, error) {
	var resp *http.Response
	err := withRetry(ctx, func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err = client.Do(req)
		return err
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

func slugify(t string) string {
	t = strings.ToLower(t)
	return strings.Trim(regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(t, "-"), "-")
}

func logToRedis(ctx context.Context, queue, object string, sku string, stage string, message string) {
    if rdb != nil {
		rdb.LPush(ctx, queue, fmt.Sprintf("%s Feed | %s: %s - %s.", object, sku, stage, message))
	}
}

func appendShopLog(ctx context.Context, key string, message string, token string) error {
	shopQuery := `
		{ 
			shop { 
				id
				logMetafield: metafield(namespace: "custom", key: "` + key + `") {
					value
				}
			} 
		}
	`
	shopResp, err := sendGraphQL(ctx, shopQuery, nil, token)
	if err != nil || shopResp.Data.Shop.ID == "" {
		return fmt.Errorf("failed to get shop data for logging: %v", err)
	}
	shopID := shopResp.Data.Shop.ID

	var logs []string
	existingValue := shopResp.Data.Shop.LogMetafield.Value
	if existingValue != "" && existingValue != "null" {
		err := json.Unmarshal([]byte(existingValue), &logs)
		if err != nil {
			log.Printf("Warning: could not parse existing logs, resetting: %v", err)
			logs = []string{}
		}
	}

	logs = append(logs, message)
	updatedLogs, _ := json.Marshal(logs)

	mutation := `
	mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
		metafieldsSet(metafields: $metafields) {
			userErrors { field message }
		}
	}`

	vars := map[string]interface{}{
		"metafields": []map[string]interface{}{
			{
				"namespace": "custom",
				"key":       key,
				"value":     string(updatedLogs),
				"ownerId":   shopID,
				"type":      "json",
			},
		},
	}

	_, err = sendGraphQL(ctx, mutation, vars, token)
	if err != nil {
		return fmt.Errorf("gql error in appendShopLog: %v", err)
	}

	return nil
}

func withRetry(ctx context.Context, operation func() error) error {
	backoff := 1 * time.Second
	maxBackoff := 32 * time.Second
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		err := operation()
		if err == nil {
			return nil
		}

		log.Printf("Attempt %d failed: %v. Retrying in %v...", i+1, err, backoff)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
	return fmt.Errorf("operation failed after max retries")
}