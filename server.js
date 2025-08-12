import express from 'express';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
// Note: CSV conversion is implemented manually; no external library is required.
import ExcelJS from 'exceljs';

// Load environment variables from .env file if present
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Parse JSON and URL‑encoded bodies
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// Store the most recent result in memory. In a production application
// you might use a database or user session instead.
let lastResult = null;

/**
 * Helper function to run an Apify actor synchronously and return its dataset items.
 *
 * @param {string} searchUrl The mobile.de search or listing URL provided by the user.
 * @param {number} maxItems The maximum number of listings to fetch. Must be >0.
 * @returns {Promise<Array<Object>>} Resolves with an array of result objects.
 */
async function runApify(searchUrl, maxItems) {
  // Support both APIFY_TOKEN and APIFY_API_TOKEN for backward compatibility
  const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
  const actorId = process.env.APIFY_ACTOR_ID;
  const useActor = String(process.env.APIFY_USE_ACTOR || 'true').toLowerCase() === 'true';
  if (!token) {
    throw new Error('APIFY_TOKEN (or APIFY_API_TOKEN) must be set in the environment.');
  }
  if (useActor && !actorId) {
    throw new Error('APIFY_ACTOR_ID must be provided when APIFY_USE_ACTOR is true.');
  }

  // Build actor input
  const input = {
    start_urls: [searchUrl],
    max_items: maxItems,
  };

  // If APIFY_USE_ACTOR is true, run the actor synchronously.
  if (useActor) {
    const endpoint = `https://api.apify.com/v2/acts/${encodeURIComponent(actorId)}/run-sync-get-dataset-items?token=${token}`;
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Apify actor request failed: ${response.status} ${response.statusText} - ${text}`);
    }
    const data = await response.json();
    return data;
  }

  // If not using an actor, treat searchUrl as a dataset ID or dataset URL.  This
  // mode allows users to fetch items from a preexisting dataset.  When
  // searchUrl is a dataset ID, we construct the dataset API URL.  When it is
  // a full URL, we assume it already points to the dataset items endpoint.
  let datasetUrl;
  if (/^https?:\/\//i.test(searchUrl)) {
    datasetUrl = searchUrl;
  } else {
    // Build dataset items URL using provided ID
    datasetUrl = `https://api.apify.com/v2/datasets/${searchUrl}/items?format=json&clean=1&token=${token}`;
  }
  // Append limit if provided
  const urlWithLimit = `${datasetUrl}${datasetUrl.includes('?') ? '&' : '?'}limit=${maxItems}`;
  const response = await fetch(urlWithLimit);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Apify dataset request failed: ${response.status} ${response.statusText} - ${text}`);
  }
  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

/**
 * Render a simple HTML page with a form for the user to input a search URL and limit.
 */
function renderFormPage(message = '', resultCount = 0) {
  return `<!DOCTYPE html>
  <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Mobile.de Crawler</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 600px; margin: auto; }
        h1 { text-align: center; }
        form { margin-top: 20px; }
        label { display: block; margin-bottom: 8px; font-weight: bold; }
        input[type="text"], input[type="number"] { width: 100%; padding: 8px; margin-bottom: 16px; box-sizing: border-box; }
        input[type="submit"] { background-color: #0070f3; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        input[type="submit"]:hover { background-color: #005bb5; }
        .message { margin-top: 20px; color: green; }
        .error { margin-top: 20px; color: red; }
        .downloads { margin-top: 20px; }
        a.download-link { display: inline-block; margin-right: 10px; color: #0070f3; text-decoration: none; }
        a.download-link:hover { text-decoration: underline; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>Mobile.de Data Crawler</h1>
        <form method="POST" action="/crawl">
          <label for="searchUrl">Mobile.de search URL:</label>
          <input type="text" id="searchUrl" name="searchUrl" required placeholder="https://www.mobile.de/..." />
          <label for="maxItems">Maximum items to fetch:</label>
          <input type="number" id="maxItems" name="maxItems" min="1" max="1000" value="50" />
          <input type="submit" value="Fetch Data" />
        </form>
        ${message ? `<p class="${resultCount > 0 ? 'message' : 'error'}">${message}</p>` : ''}
        ${resultCount > 0 ? `<div class="downloads">
          <p>Download your results:</p>
          <a class="download-link" href="/download/json">JSON</a>
          <a class="download-link" href="/download/csv">CSV</a>
          <a class="download-link" href="/download/excel">Excel</a>
        </div>` : ''}
      </div>
    </body>
  </html>`;
}

// GET home page: show form
app.get('/', (req, res) => {
  const count = lastResult ? lastResult.length : 0;
  const message = count > 0 ? `Successfully fetched ${count} items. Use the links below to download your data.` : '';
  res.send(renderFormPage(message, count));
});

// POST /crawl: call Apify and store results
app.post('/crawl', async (req, res) => {
  const { searchUrl, maxItems } = req.body;
  if (!searchUrl) {
    res.status(400).send(renderFormPage('Please provide a search URL.', 0));
    return;
  }
  // Limit the maxItems to a reasonable number to avoid heavy API loads
  const limit = parseInt(maxItems, 10) || 50;
  try {
    const data = await runApify(searchUrl, limit);
    lastResult = data;
    res.send(renderFormPage(`Successfully fetched ${data.length} items.`, data.length));
  } catch (err) {
    console.error(err);
    res.status(500).send(renderFormPage(`Error fetching data: ${err.message}`, 0));
  }
});

// GET /download/json: return JSON data
app.get('/download/json', (req, res) => {
  if (!lastResult) {
    res.redirect('/');
    return;
  }
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Content-Disposition', 'attachment; filename="mobile-de-data.json"');
  res.send(JSON.stringify(lastResult, null, 2));
});

// GET /download/csv: convert JSON to CSV. Implement conversion manually to avoid external dependencies.
app.get('/download/csv', (req, res) => {
  if (!lastResult) {
    res.redirect('/');
    return;
  }
  try {
    // Determine the header order from the keys of the first item
    const headers = Object.keys(lastResult[0] || {});
    const escape = (value) => {
      if (value === null || value === undefined) return '';
      const str = String(value);
      // Escape double quotes by doubling them
      const escaped = str.replace(/"/g, '""');
      // Wrap fields containing commas, quotes or newlines in double quotes
      return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
    };
    const rows = [headers.join(',')];
    lastResult.forEach(item => {
      const row = headers.map(h => escape(item[h]));
      rows.push(row.join(','));
    });
    const csv = rows.join('\n');
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="mobile-de-data.csv"');
    res.send(csv);
  } catch (err) {
    res.status(500).send(`Error converting data to CSV: ${err.message}`);
  }
});

// GET /download/excel: convert JSON to Excel
app.get('/download/excel', async (req, res) => {
  if (!lastResult) {
    res.redirect('/');
    return;
  }
  try {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Data');
    // Add header row
    const headers = Object.keys(lastResult[0] || {});
    worksheet.addRow(headers);
    // Add data rows
    lastResult.forEach(item => {
      const row = headers.map(key => item[key]);
      worksheet.addRow(row);
    });
    // Generate buffer
    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="mobile-de-data.xlsx"');
    res.send(Buffer.from(buffer));
  } catch (err) {
    res.status(500).send(`Error converting data to Excel: ${err.message}`);
  }
});

// Start server
app.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`);
});