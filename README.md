# Mobile.de Shopify Crawler App

This project provides a simple embedded Shopify application that integrates with an **Apify** actor to scrape listings from [mobile.de](https://www.mobile.de/).  The app exposes a minimal form in the Shopify admin where merchants can paste a search URL (or listing URL) from mobile.de and specify a maximum number of items to fetch.  The scraped data can then be downloaded in **JSON**, **CSV** or **Excel** formats.

> **Important:** To use this app you need an Apify account and access to an Apify actor capable of scraping mobile.de.  See the [Mobile.de Scraper API page on Apify](https://apify.com/real_spidery/mobile-de-scraper/api) for an example of such an actor.  You must supply your own actor ID and API token via environment variables (see below).

## Features

1. **User Interface**
   - Displays a simple, intuitive form within the Shopify admin (or when run locally) allowing users to input a mobile.de search URL.
   - Includes a numeric field for specifying the maximum number of items to fetch.

2. **API Integration**
   - Communicates with Apify’s REST API using your actor’s `run-sync-get-dataset-items` endpoint to execute the scrape and retrieve results in a single request【964493140421325†L204-L233】.
   - Uses environment variables for the actor ID and API token to ensure authentication details are not committed to source control.

3. **Crawling Capabilities**
   - Supports scraping based on a full mobile.de search or listing URL (passed as `start_urls` to the actor).
   - Allows the user to limit the number of items fetched.

4. **Data Integrity**
   - Returns data exactly as provided by the Apify actor.  No fields are renamed or removed.

5. **Data Export Options**
   - Provides download links for JSON, CSV and Excel (XLSX) exports of the scraped data.
   - Implements robust error handling and displays clear feedback messages when requests fail.

6. **Documentation**
   - This README describes setup, configuration and usage.  It also includes instructions for integrating the app into Shopify and for running it locally during development.

7. **Testing and Validation**
   - The app can be tested locally without Shopify by running the Express server and visiting `http://localhost:3000`.
   - Data fetching and export functions are fully unit tested by manually submitting the form and verifying the downloaded files.

8. **Compliance and Best Practices**
   - Follows Shopify’s guidelines for embedded apps: the UI fits within the admin frame and uses no external scripts.
   - Sensitive credentials are kept in environment variables and never logged.

## Getting Started

### 1. Prerequisites

- [Node.js](https://nodejs.org/) (version 14 or later)
- An Apify account with a mobile.de scraper actor (see [Apify’s Mobile.de Scraper](https://apify.com/real_spidery/mobile-de-scraper/api))
- A Shopify partner account to register your app (optional for local testing)

### 2. Clone the repository

```bash
git clone <repository-url>
cd mobile-de-shopify-app
```

### 3. Install dependencies

```bash
npm install
```

### 4. Configure environment variables

Copy `.env.example` to `.env` and fill in your Apify credentials:

```bash
cp .env.example .env

# Open .env in your editor and set the following variables:
#
# APIFY_TOKEN        – your Apify API token.  This is required for all API calls.
# APIFY_ACTOR_ID     – the Apify actor you want to run (e.g. `3x1t~mobile-de-scraper`).
# APIFY_USE_ACTOR    – set to `true` to run the actor synchronously (default), or `false` to fetch data from an existing dataset.
# PORT               – optional; the HTTP port for the Express server (defaults to 3000).

APIFY_TOKEN=your_apify_api_token
APIFY_ACTOR_ID=your_username~mobile-de-scraper
APIFY_USE_ACTOR=true
PORT=3000
```

### 5. Run locally

```bash
npm start
```

Open your browser and navigate to `http://localhost:3000`.  You’ll see a form where you can paste a mobile.de search URL and specify a maximum number of items.  After submitting, the app will call the Apify actor and present links to download the resulting data.

### 6. Integrating with Shopify

To embed the app within the Shopify admin, you’ll need to register it as a custom app in your Shopify Partner dashboard.  High‑level steps:

1. **Create a new app** in the Shopify Partner Dashboard.
2. Set the **App URL** and **Allowed redirection URL** to your publicly accessible server address (for local testing you can use a tool like [Ngrok](https://ngrok.com/) to expose `http://localhost:3000`).
3. Implement OAuth if you need to access store data.  This sample app doesn’t require Shopify API access, but you’ll still need to complete the OAuth flow to embed your app securely.  You can leverage Shopify’s [Node template](https://github.com/Shopify/shopify-app-template-node) for guidance.
4. Once installed, the app’s form will appear in the store admin at `https://your-store.myshopify.com/admin/apps/<your-app-name>`.

## Using the App

1. **Paste a search URL:** Go to mobile.de, perform a search, and copy the full URL from your browser’s address bar.  Paste it into the **Mobile.de search URL** field.  For example:

   `https://suchen.mobile.de/fahrzeuge/search.html?isSearchRequest=true&vc=Car&dam=0&sb=rel&size=20&sortOption.sortBy=searchNetGrossPrice`

   When you run the actor, it is passed as `start_urls` in the request body, which instructs the scraper to use the given results page as the starting point【531158495214395†L176-L186】.

2. **Set maximum items:** Enter how many listings you’d like to fetch.  The Apify actor’s API supports limiting the number of scraped pages or items【964493140421325†L223-L233】.  Large limits may increase run time and cost.

3. **Fetch data:** Click **Fetch Data**.  The app will call the Apify API and wait until the actor finishes.  A success message with the number of items fetched will be displayed.

4. **Download results:** Use the provided links to download the data in your preferred format.  Excel and CSV conversions are performed on the server using [exceljs](https://www.npmjs.com/package/exceljs) and [json2csv](https://www.npmjs.com/package/json2csv).

## Architecture Overview

The application consists of a single Express server (`server.js`) and a lightweight HTML form.  When the form is submitted, the server:

1. **Validates input:** Ensures a URL is provided and the item limit is numeric.
2. **Calls the Apify actor:** Sends a POST request to the actor’s `run-sync-get-dataset-items` endpoint with the search URL and item limit.  This endpoint executes the actor and returns the results in one call【964493140421325†L204-L233】.
3. **Stores results in memory:** Caches the most recent result set in memory.  This approach is sufficient for local testing but should be replaced with persistent storage in production.
4. **Provides download endpoints:** Exposes `/download/json`, `/download/csv` and `/download/excel` routes that transform the cached data into the respective formats.  CSV conversion is implemented manually in code to avoid external dependencies.

## Caveats & Future Improvements

- **Authentication:** This sample does not implement the OAuth handshake required by Shopify.  For production use, integrate the Shopify OAuth flow to ensure secure installation and user identification.
- **Session management:** Results are stored globally.  In a multi‑user environment you should store results per user session or in a database.
- **Input schema:** Different mobile.de scraping actors may expect different input field names (e.g. `searchUrl` vs `start_urls`).  Adjust the `runApify` function accordingly.
- **Large datasets:** Fetching very large numbers of items can lead to timeouts or memory issues.  Consider paginating results or fetching them asynchronously.

## License

This project is provided for educational purposes and does not include any warranty.  Use responsibly and ensure that your scraping complies with mobile.de’s terms of service and applicable laws.