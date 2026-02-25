# Shopify AI Feed
This project is a high-performance, distributed system designed to automate Shopify product content generation using Google Gemini 2.0 Flash. It consists of a Producer and Worker architecture connected via Redis, ensuring efficient handling of rate limits and parallel processing.

## 🚀 Overview
The system automates the enrichment of Shopify products by:
1. **Scanning** your Shopify store for products that need updates.
2. **Analyzing** product images and metadata using AI.
3. **Generating** SEO-optimized titles, descriptions, tags, and alt text.
4. **Syncing** the data back to Shopify and logging results to Shopify Shop metafields.

## 🛠 Architecture

1. **Producer (`producer.go`)**
The Producer acts as the "scanner." It iterates through your Shopify catalog using GraphQL cursor pagination.
- **Smart Filtering**: It compares the `updated_at` timestamps of product/image metafields against the `processTime` in the `ai_json` metafield.
- **Queueing**: Products requiring updates (or new products) are pushed into a Redis list (`ai_queue`).
- **Rate Limit Awareness**: Respects Shopify's GraphQL cost limits by sleeping when the throttle status is low.

2. **Redis**
Acts as the message broker between the Producer and the Workers. It handles:
- `ai_queue`: The list of product IDs to be processed.
- `info_logs` & `error_logs`: Temporary storage for logs before they are synced to Shopify.

3. **Worker (`worker.go`)**
The Worker handles the heavy lifting. You can run multiple instances to scale processing.
- **Gemini Integration**: Downloads product images and sends them along with metadata to Gemini 2.0 Flash.
- **Multimodal Analysis**: The AI "sees" the product images to generate accurate alt text and keywords.
- **Shopify Sync**: Updates the product's Title, Handle, Description, SEO tags, and various custom Metafields (Tone, Recipient, Ratings).
- **Graceful Shutdown**: Handles `SIGTERM`/`SIGINT` to finish current tasks before exiting

## ⚙️ Configuration
The application requires the following environment variables:
| Variable | Description |
| -------- | ----------- |
| `SHOPIFY_SHOP_URL` | Your shop domain (e.g., `your-store.myshopify.com`) |
| `SHOPIFY_CLIENT_ID` | Shopify App API Key |
| `SHOPIFY_CLIENT_SECRET` | Shopify App Admin Secret |
| `SHOPIFY_API_VERSION` | Default: `2026-01` |
| `REDIS_URL` | Connection string (e.g., `redis://localhost:6379`) |
| `GEMINI_API_KEY` | Your Google AI Studio API Key |

## 📦 Data Flow
1. **Produce**: `producer` finds a product with outdated AI content -> Pushes ID to Redis.
2. **Consume**: `worker` pops the ID -> Fetches full details from Shopify.
3. **AI Process**: `worker` sends image Base64 data + Metafields to Gemini.
4. **Update**: `worker` applies the mutation to Shopify:
    - **SEO**: Updates Title/Handle/Meta-Description.
    - **Metafields**: Updates `custom.ai_json` with a timestamp to prevent re-processing.
    - **Images**: Updates Alt text for every image via fileUpdate.
5. **Log**: Resulting success/fail messages are sent back to Redis and eventually appended to the Shopify Shop metafields (`log_info`/`log_error`).

## 🛠 Setup & Running
**Prerequisites**
- Go 1.20+
- A running Redis instance
- A Shopify Custom App with `write_products` and `write_files` scopes

**Running the Producer**
```shell
go run producer.go
```

**Running the Worker**
```shell
go run worker.go
```

## 📝 Important Notes
- **Slugification**: The worker automatically generates URL-friendly handles using a `slugify` function to maintain SEO integrity.
- **Safety Ratings**: The Gemini prompt includes instructions to rate content for Nudity, Politics, and Language, stored in Shopify for filtering.
- **Retry Logic**: The system uses an exponential backoff retry mechanism for network-sensitive operations (Shopify API and Gemini).
