/*
 * mobile-de-shopify-app/server.js
 *
 * This file implements the backend for a simple Shopify embedded application
 * that integrates with an Apify actor to crawl listings from mobile.de. It
 * provides a small HTML interface for entering a search URL and a maximum
 * number of items to fetch, kicks off an asynchronous Apify actor run,
 * monitors its status and ultimately exposes download links for the raw
 * and normalized results in JSON, CSV and Excel formats. Normalization
 * converts arrays of images, features and attributes into flat fields and
 * JSON strings suitable for importing into Shopify metafields.
 */

import express from 'express';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import ExcelJS from 'exceljs';
import fs from 'fs';
import path from 'path';


// Load environment variables from .env if present.
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Enable JSON and URL-encoded body parsing for incoming requests.
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve static assets from the `public` directory.  This allows us to place
// a custom index.html with a richer user interface (progress bar, status
// polling and download links) into the public folder.  If the file is
// missing the route defined below will still serve a basic HTML form.
app.use(express.static('public'));

/*
 * Helpers to safely parse arrays or JSON strings. The Apify actor returns
 * some properties either as arrays, objects or strings with delimiters. To
 * build clean metafields we need to coerce these into arrays. If parsing
 * fails we fall back to splitting on semicolons or commas.
 */
function safeParseMaybeList(value) {
  if (value === null || value === undefined) return [];
  // Already an array – return as-is.
  if (Array.isArray(value)) return value;
  const str = String(value).trim();
  if (!str) return [];
  try {
    const parsed = JSON.parse(str);
    if (Array.isArray(parsed)) return parsed;
    if (parsed && typeof parsed === 'object') return [parsed];
    return [];
  } catch (err) {
    // Fall back to splitting on semicolons or commas.
    if (/[;,]/.test(str)) {
      return str.split(/[;,]\s*/).filter(Boolean);
    }
    return [str];
  }
}

/*
 * Normalise a single item from the Apify dataset. This function flattens
 * nested structures and extracts useful fields such as price, dealer
 * information, images, features and attributes. It also preserves the
 * original arrays as JSON strings for advanced use while producing
 * individual fields (image_1..image_10, feature_1..feature_10,
 * attribute_1_name/value pairs) for convenience.
 */
function normalizeItem(item = {}) {
  const out = {};

  // Title and URL.
  out.title = item.title ?? '';
  out.url = item.url ?? '';
  out.preview_image = item.previewImage ?? '';

  // Price: Apify may return { amount, currency } or a string like "€ 39.900".
  const price = item.price;
  if (price && typeof price === 'object') {
    out.price_amount = price.amount ?? price.value ?? '';
    out.price_currency = price.currency ?? '';
  } else if (typeof price === 'string') {
    const amountMatch = price.match(/[\d.,]+/);
    const currencyMatch = price.match(/[€$£]|RON|EUR|USD|GBP/i);
    out.price_amount = amountMatch ? amountMatch[0] : '';
    out.price_currency = currencyMatch ? currencyMatch[0] : '';
  } else {
    out.price_amount = '';
    out.price_currency = '';
  }

  // Images: convert to array of URLs. Each entry may be an object with a url
  // property or a plain string.
  const images = safeParseMaybeList(item.images).map((img) => {
    if (img && typeof img === 'object') return img.url || img.src || '';
    return String(img);
  });
  out.images_json = JSON.stringify(images);
  images.slice(0, 10).forEach((u, idx) => {
    out[`image_${idx + 1}`] = u;
  });

  // Features: array of strings or objects. Flatten into feature_1..feature_10.
  const features = safeParseMaybeList(item.features).map((feat) => {
    if (feat && typeof feat === 'object') return feat.name || feat.title || '';
    return String(feat);
  });
  out.features_json = JSON.stringify(features);
  features.slice(0, 10).forEach((f, idx) => {
    out[`feature_${idx + 1}`] = f;
  });

  // Attributes: array of {name,value} objects or strings like "name:value; ...".
  let attrs = [];
  const rawAttrs = safeParseMaybeList(item.attributes);
  if (
    rawAttrs.length &&
    typeof rawAttrs[0] === 'object' &&
    (Object.prototype.hasOwnProperty.call(rawAttrs[0], 'name') ||
      Object.prototype.hasOwnProperty.call(rawAttrs[0], 'key'))
  ) {
    attrs = rawAttrs.map((a) => ({
      name: a.name || a.key || '',
      value: a.value ?? ''
    }));
  } else if (rawAttrs.length) {
    attrs = rawAttrs
      .flatMap((s) => String(s).split(/[;|]/))
      .map((kv) => {
        const [k, ...rest] = kv.split(':');
        return { name: (k || '').trim(), value: (rest.join(':') || '').trim() };
      })
      .filter((a) => a.name || a.value);
  }
  out.attributes_json = JSON.stringify(attrs);
  attrs.slice(0, 10).forEach((a, idx) => {
    out[`attribute_${idx + 1}_name`] = a.name || '';
    out[`attribute_${idx + 1}_value`] = a.value || '';
  });

  // Dealer details: may be an object or a JSON string.
  try {
    const dealer = typeof item.dealerDetails === 'string'
      ? JSON.parse(item.dealerDetails)
      : item.dealerDetails || {};
    out.dealer_name = dealer.name || '';
    out.dealer_city = dealer.city || dealer.location || '';
    out.dealer_phone = dealer.phone || dealer.telephone || '';
  } catch (err) {
    out.dealer_name = '';
    out.dealer_city = '';
    out.dealer_phone = '';
  }

  // Additional metadata fields.
  out.source_id = item.id ?? '';
  out.seller_id = item.sellerId ?? '';
  out.segment = item.segment ?? '';
  out.category = item.category ?? '';
  out.rank = item.rank ?? '';

  return out;
}

/*
 * Expand an item's attributes into separate columns.  Each attribute in
 * the `attributes` array (or string) is mapped to a column named by
 * the attribute's name, containing the corresponding value.  This
 * allows the final export to include each attribute as its own column,
 * rather than generic attribute_1_name/value pairs.  Only attributes
 * that have a defined name are included.
 */
function expandAttributesToColumns(item) {
  const cols = {};
  // Attempt to parse the raw attributes from the item.  Attributes may be
  // provided as a plain object, a JSON string representing an object or
  // an array of objects with `name`/`value` properties.  We normalise
  // all of these forms into an array of { name, value } pairs for
  // simplified processing.
  let rawAttrs;
  const attrRaw = item && item.attributes;
  if (attrRaw && typeof attrRaw === 'object' && !Array.isArray(attrRaw)) {
    // Plain object: convert to array of { name, value }.
    rawAttrs = Object.entries(attrRaw).map(([k, v]) => ({ name: k, value: v }));
  } else if (attrRaw && typeof attrRaw === 'string') {
    // JSON string: try to parse as object.
    try {
      const parsed = JSON.parse(attrRaw);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        rawAttrs = Object.entries(parsed).map(([k, v]) => ({ name: k, value: v }));
      }
    } catch (e) {
      // leave rawAttrs undefined; we'll fall back to safeParseMaybeList
    }
  }
  // Fallback: use safeParseMaybeList on the raw attribute value.  This
  // handles arrays of objects or strings like "name:value" separated by
  // semicolons or pipes.  If item.attributes is also a plain object we
  // convert it here too to cover any missed cases.
  if (!rawAttrs) {
    rawAttrs = safeParseMaybeList(attrRaw);
    if (
      item &&
      item.attributes &&
      typeof item.attributes === 'object' &&
      !Array.isArray(item.attributes)
    ) {
      rawAttrs = Object.entries(item.attributes).map(([k, v]) => ({ name: k, value: v }));
    }
  }
  let attrs = [];
  // If the first element is an object with a `name` or `key` field, treat
  // it directly as our list of attribute objects.  Otherwise, attempt to
  // parse strings like "name:value" separated by semicolons or pipes.
  if (
    rawAttrs.length &&
    typeof rawAttrs[0] === 'object' &&
    (Object.prototype.hasOwnProperty.call(rawAttrs[0], 'name') ||
     Object.prototype.hasOwnProperty.call(rawAttrs[0], 'key'))
  ) {
    attrs = rawAttrs.map((a) => ({
      name: a.name || a.key || '',
      value: a.value ?? ''
    }));
  } else if (rawAttrs.length) {
    // Flatten any string lists like "name:value" separated by semicolons or pipes.
    attrs = rawAttrs
      .flatMap((s) => String(s).split(/[;|]/))
      .map((kv) => {
        const [k, ...rest] = kv.split(':');
        return {
          name: (k || '').trim(),
          value: (rest.join(':') || '').trim()
        };
      })
      .filter((a) => a.name || a.value);
  }
  // Convert each attribute to a separate column.  We derive the
  // displayed value by extracting meaningful tokens from the raw
  // attribute value: numeric sequences are kept intact (e.g. '1,395 ccm'
  // → '1,395'), otherwise the first word or first part before a comma
  // is used (e.g. 'Used vehicle' → 'Used').
  attrs.forEach((attr) => {
    const name = attr.name ? String(attr.name).trim() : '';
    if (!name) return;
    let val = attr.value;
    // If the value is an array, use its first element.
    if (Array.isArray(val) && val.length > 0) {
      val = val[0];
    }
    let strVal = val === undefined || val === null ? '' : String(val).trim();
    let shortVal = '';
    // Step 1: If the value starts with a digit, handle numeric and alpha-numeric cases.
    if (/^[0-9]/.test(strVal)) {
      // Consider the first whitespace-delimited token.  If it contains
      // alphabetic characters, keep the whole token (e.g. '0257E').
      const firstToken = strVal.split(/\s+/)[0];
      if (/[A-Za-z]/.test(firstToken)) {
        shortVal = firstToken;
      } else {
        // Pure numeric token: extract digits, commas, periods and slashes.
        const m = strVal.match(/^[0-9][0-9,./]*/);
        shortVal = m ? m[0] : strVal;
      }
    } else {
      // Step 2: Check for comma-separated values. If text after comma does not start with a digit,
      // treat it as a delimiter and use the first word from the first part.
      const commaIndex = strVal.indexOf(',');
      if (commaIndex > -1) {
        const second = strVal.slice(commaIndex + 1).trim();
        if (second && !/^[0-9]/.test(second)) {
          const firstPart = strVal.slice(0, commaIndex).trim();
          const firstWord = firstPart.split(/\s+/)[0];
          shortVal = firstWord;
        }
      }
      // Step 3: If still empty, default to the first word of the value.
      if (!shortVal) {
        const parts = strVal.split(/\s+/);
        shortVal = parts.length > 0 ? parts[0] : strVal;
      }
    }
    // Assign the short value to the column if not already set.
    if (!Object.prototype.hasOwnProperty.call(cols, name)) {
      cols[name] = shortVal;
    }
  });
  return cols;
}

/*
 * Convert an array of objects to a CSV string. All keys across the array
 * will become columns. Values are escaped for commas, quotes and newlines.
 */
function toCsv(rows) {
  if (!rows || rows.length === 0) return '';
  // Determine all unique headers.
  const headerSet = new Set();
  rows.forEach((row) => Object.keys(row).forEach((key) => headerSet.add(key)));
  const headers = Array.from(headerSet);
  // Helper to escape CSV cells.
  const escapeCell = (val) => {
    const str = val === undefined || val === null ? '' : String(val);
    const escaped = str.replace(/"/g, '""');
    return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
  };
  const lines = [];
  lines.push(headers.join(','));
  for (const row of rows) {
    lines.push(headers.map((h) => escapeCell(row[h])).join(','));
  }
  return lines.join('\n');
}

/*
 * Load a Shopify mapping file from Excel.  The mapping file should
 * contain two columns: "Shopify" and "Source".  Each row maps a
 * Shopify column name to a source field name.  Empty or missing
 * entries are ignored.  The returned object includes both a
 * dictionary (mapping) and an ordered array of Shopify column names
 * preserving the order they appear in the file.  If the file is not
 * found or cannot be parsed, an error is thrown.
 */
async function loadMapping() {
  const mappingPath = process.env.SHOPIFY_MAPPING_FILE || 'mapping.xlsx';
  if (!fs.existsSync(mappingPath)) {
    throw new Error(`Mapping file not found at ${mappingPath}`);
  }
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(mappingPath);
  const worksheet = workbook.worksheets[0];
  const mapping = {};
  const order = [];
  // Find header indices for 'Shopify' and 'Source'.
  let shopifyIdx = -1;
  let sourceIdx = -1;
  worksheet.getRow(1).eachCell((cell, colNumber) => {
    const val = String(cell.value || '').trim().toLowerCase();
    if (val === 'shopify') shopifyIdx = colNumber;
    if (val === 'source') sourceIdx = colNumber;
  });
  if (shopifyIdx === -1 || sourceIdx === -1) {
    throw new Error(`Mapping file must contain 'Shopify' and 'Source' columns`);
  }
  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return; // skip header
    const shopifyVal = row.getCell(shopifyIdx).value;
    const sourceVal = row.getCell(sourceIdx).value;
    const key = shopifyVal ? String(shopifyVal).trim() : '';
    const value = sourceVal ? String(sourceVal).trim() : '';
    if (key && value) {
      mapping[key] = value;
      order.push(key);
    }
  });
  return { mapping, order };
}

/*
 * Extract only digits from a value.  Useful for fields such as
 * displacement or mileage (e.g. '1,969 ccm' → '1969').  Returns an
 * empty string for null/undefined values.
 */
function extractNumber(value) {
  if (value === null || value === undefined) return '';
  const digits = String(value).match(/\d+/g);
  return digits ? digits.join('') : '';
}

/*
 * Extract kW or hp from a string containing both units.  For example,
 * '195 kW (265 hp)' → '195' for mode 'kw' and '265' for mode 'cp'.
 */
function extractKwHp(value, mode = 'kw') {
  if (value === null || value === undefined) return '';
  const str = String(value);
  const kwMatch = str.match(/(\d{2,4})\s*kW/i);
  const hpMatch = str.match(/\((\d{2,4})\s*hp\)/i);
  if (mode === 'kw' && kwMatch) return kwMatch[1];
  if (mode === 'cp' && hpMatch) return hpMatch[1];
  return '';
}

/*
 * Load the metafields mapping from an Excel file.  The mapping file should
 * contain a sheet named 'Metafields_Mapping' where each row has
 * 'Molbile.de' (the source field from the Apify dataset) and up to two
 * Shopify metafield columns (destinations).  This function returns an
 * array of mapping objects, each with a source and an array of dests.
 * If the file or sheet does not exist, an empty array is returned.
 */
async function loadMetafieldsMappingFile() {
  const fileName =
    process.env.METAFIELDS_MAPPING_FILE ||
    process.env.SHOPIFY_METAFIELDS_FILE ||
    'dataset_mobile-de-scraper_mapped_metafields.xlsx';
  if (!fs.existsSync(fileName)) {
    return [];
  }
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(fileName);
  const sheet = workbook.getWorksheet('Metafields_Mapping');
  if (!sheet) {
    return [];
  }
  // Determine column indexes for source and two dest columns.
  let sourceCol = -1;
  let dest1Col = -1;
  let dest2Col = -1;
  sheet.getRow(1).eachCell((cell, colNumber) => {
    const val = String(cell.value || '').trim().toLowerCase();
    if (val.startsWith('molbile')) sourceCol = colNumber;
    if (val.includes('shopify metafields 1')) dest1Col = colNumber;
    if (val.includes('shopify metafields 2')) dest2Col = colNumber;
  });
  if (sourceCol === -1) return [];
  const mappings = [];
  sheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    const src = row.getCell(sourceCol).value
      ? String(row.getCell(sourceCol).value).trim()
      : '';
    if (!src) return;
    const dest1 = dest1Col > -1 && row.getCell(dest1Col).value
      ? String(row.getCell(dest1Col).value).trim()
      : '';
    const dest2 = dest2Col > -1 && row.getCell(dest2Col).value
      ? String(row.getCell(dest2Col).value).trim()
      : '';
    const dests = [];
    if (dest1) dests.push(dest1);
    if (dest2) dests.push(dest2);
    if (dests.length) {
      mappings.push({ source: src, dests });
    }
  });
  return mappings;
}

/*
 * Build a Shopify metafields row from a raw dataset item using the provided
 * mapping list.  This function assigns dataset values to their mapped
 * metafield names, accumulates tags, generates default columns such as
 * Title, Variant SKU/ID, Image Src and Variant Price, and adds brand and
 * model to both metafields and tags.  It also handles special parsing
 * for power values (kW and hp).  The returned object represents a single
 * row for the final Shopify import.
 */
function buildMetafieldsRow(item, mappingList) {
  const row = {};
  const tagsSet = new Set();
  // Apply mappings
  mappingList.forEach(({ source, dests }) => {
    const value = item ? item[source] : undefined;
    if (value === undefined || value === null || value === '') return;
    dests.forEach((dest) => {
      if (!dest) return;
      const destTrim = dest.trim();
      // Tags accumulation
      if (destTrim === 'Tags') {
        tagsSet.add(String(value));
        return;
      }
      // Image Alt Text
      if (destTrim === 'Image Alt Text') {
        row['Image Alt Text'] = value;
        return;
      }
      // Vendor
      if (destTrim === 'Vendor') {
        row['Vendor'] = value;
        return;
      }
      // Variant Price
      if (destTrim === 'Variant Price') {
        row['Variant Price'] = value;
        return;
      }
      // Metafields
      if (destTrim.startsWith('Metafield')) {
        // Special-case power: extract kW and hp
        if (source === 'attributes/Power') {
          if (destTrim.includes('putere_cp')) {
            row[destTrim] = extractKwHp(value, 'cp');
          } else if (destTrim.includes('putere_kw')) {
            row[destTrim] = extractKwHp(value, 'kw');
          } else {
            row[destTrim] = value;
          }
        } else {
          row[destTrim] = value;
        }
        return;
      }
      // Default: assign value to destination field
      row[destTrim] = value;
    });
  });
  // Add brand and model metafields if present.
  if (item && item.brand) {
    row['Metafield: custom.marca [single_line_text_field]'] = item.brand;
    tagsSet.add(String(item.brand));
  }
  if (item && item.model) {
    row['Metafield: custom.model [single_line_text_field]'] = item.model;
    tagsSet.add(String(item.model));
  }
  // Title
  row['Title'] = (item && item.title) ? String(item.title) : '';
  // Variant SKU / ID
  const idVal = item && item.id !== undefined ? String(item.id) : '';
  row['Variant SKU'] = idVal;
  row['Variant ID'] = idVal;
  // Variant Price: if not filled by mapping, derive from price object/fields.
  if (!row['Variant Price']) {
    let priceVal = '';
    if (item && typeof item.price === 'object' && item.price !== null) {
      priceVal = item.price.total?.amount ||
                 item.price.amount ||
                 item.price.value ||
                 item['price/total/amount'] ||
                 '';
    } else if (item) {
      priceVal = item['price/total/amount'] || '';
    }
    row['Variant Price'] = priceVal;
  }
  // Vendor: if not filled, set to sellerId if available.
  if (!row['Vendor'] && item && item.sellerId !== undefined) {
    row['Vendor'] = String(item.sellerId);
  }
  // Image Src: pick the first available image field.
  let img = '';
  if (item) {
    for (let i = 0; i < 50; i++) {
      const key = `images/${i}`;
      if (item[key]) {
        img = item[key];
        break;
      }
    }
    if (!img && Array.isArray(item.images) && item.images.length > 0) {
      const firstImg = item.images[0];
      if (typeof firstImg === 'object' && firstImg !== null) {
        img = firstImg.url || firstImg.src || '';
      } else {
        img = String(firstImg);
      }
    }
  }
  row['Image Src'] = img;
  // Image Alt Text: default to title if not explicitly set.
  if (!row['Image Alt Text']) {
    row['Image Alt Text'] = row['Title'] || '';
  }
  // Tags: join unique tags
  row['Tags'] = Array.from(tagsSet).filter(Boolean).join(', ');
  // Body HTML: include description if present.
  row['Body HTML'] = (item && item.description) ? String(item.description) : '';
  // Features: build an HTML list from item.features if available.
  if (item && item.features) {
    const feats = safeParseMaybeList(item.features).map((f) => {
      if (f && typeof f === 'object') return f.name || f.title || '';
      return String(f);
    }).filter(Boolean);
    row['Features'] =
      feats.length > 0
        ? '<ul><li>' + feats.join('</li><li>') + '</li></ul>'
        : '';
  } else {
    row['Features'] = '';
  }
  // Expand raw attributes into separate columns.  Each attribute name will
  // become a column containing the attribute's value.  This preserves
  // attribute data in a human-readable form alongside metafields.  The
  // mapping-based columns are left intact.  We merge after all
  // other fields to avoid overriding any explicitly mapped fields.
  const attributeColumns = expandAttributesToColumns(item);
  Object.keys(attributeColumns).forEach((key) => {
    // Only add if the key is not already used in the row.
    if (!Object.prototype.hasOwnProperty.call(row, key)) {
      row[key] = attributeColumns[key];
    }
  });
  return row;
}

/*
 * Map an entire array of raw dataset items to Shopify metafields rows using
 * a provided mapping list.  Returns a new array of objects.
 */
function mapDatasetToMetafields(items, mappingList) {
  return items.map((item) => buildMetafieldsRow(item, mappingList));
}

/*
 * Map a dataset record to a full Shopify row based on a provided
 * mapping object.  Special columns such as Handle, Tags, Body HTML and
 * certain metafields are generated dynamically.  Any Shopify columns
 * not present in the mapping will be left empty.
 */
function mapItemToShopifyTemplate(item, mapping) {
  const row = {};
  // Precompute normalised base fields and slug for handle.
  const base = normalizeItem(item);
  const rawTitle = (item.title || '').toString().trim();
  let slug = rawTitle
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  const idStr = item.id !== undefined && item.id !== null ? String(item.id).trim() : '';
  if (slug && idStr) slug = `${slug}-${idStr}`;
  else if (idStr) slug = idStr;
  // Tags combine brand and model if available.
  const tags = [item.brand, item.model]
    .filter((v) => v && String(v).trim())
    .map((v) => String(v).trim())
    .join(', ');
  // Build description_tag (features list) and Body HTML.
  let featuresList = [];
  try {
    featuresList = base.features_json ? JSON.parse(base.features_json) : [];
  } catch {
    featuresList = [];
  }
  const featuresHtml = featuresList && featuresList.length > 0
    ? '<ul>' + featuresList.map((f) => `<li>${String(f).trim()}</li>`).join('') + '</ul>'
    : '';
  const description = item.description || item.description_tag || '';
  const bodyHtml = description ? String(description) + (featuresHtml ? '\n' + featuresHtml : '') : featuresHtml;
  // Determine primary image.
  let primaryImage = '';
  if (base.image_1) primaryImage = base.image_1;
  else if (item.previewImage) primaryImage = item.previewImage;
  else if (Array.isArray(item.images) && item.images.length) {
    const first = item.images[0];
    primaryImage = (first && typeof first === 'object') ? (first.url || first.src || '') : String(first);
  }
  // Fill mapping columns.
  for (const shopifyCol of Object.keys(mapping)) {
    const sourceField = mapping[shopifyCol];
    // Custom generation for specific columns.
    if (shopifyCol.toLowerCase() === 'handle') {
      row[shopifyCol] = slug || '';
    } else if (shopifyCol.toLowerCase() === 'tags') {
      row[shopifyCol] = tags || '';
    } else if (shopifyCol.toLowerCase() === 'title') {
      row[shopifyCol] = rawTitle || '';
    } else if (shopifyCol.toLowerCase() === 'body html') {
      row[shopifyCol] = bodyHtml || '';
    } else if (shopifyCol.toLowerCase() === 'image src') {
      row[shopifyCol] = primaryImage || '';
    } else if (shopifyCol.toLowerCase() === 'vendor') {
      row[shopifyCol] = item.brand || item.vendor || '';
    } else if (shopifyCol.toLowerCase() === 'type') {
      row[shopifyCol] = item.category || item.segment || '';
    } else if (shopifyCol.toLowerCase() === 'variant sku') {
      row[shopifyCol] = idStr || '';
    } else if (shopifyCol.toLowerCase() === 'variant price') {
      const priceObj = item.price;
      let priceStr = '';
      if (priceObj && typeof priceObj === 'object') {
        priceStr = priceObj.amount || priceObj.value || '';
      } else if (typeof priceObj === 'string') {
        const m = priceObj.match(/[\d.,]+/);
        priceStr = m ? m[0] : '';
      }
      row[shopifyCol] = priceStr;
    } else if (shopifyCol === 'Metafield: description_tag [string]') {
      row[shopifyCol] = featuresHtml || '';
    } else if (shopifyCol === 'Metafield: custom.cilindree [single_line_text_field]') {
      // Try to extract cylinder volume from a field or attributes.
      const fieldName = sourceField;
      const val = item[fieldName] || '';
      row[shopifyCol] = extractNumber(val);
    } else if (shopifyCol === 'Metafield: custom.kilometraj [single_line_text_field]') {
      const fieldName = sourceField;
      const val = item[fieldName] || '';
      row[shopifyCol] = extractNumber(val);
    } else if (shopifyCol === 'Metafield: custom.putere_kw [single_line_text_field]') {
      const fieldName = sourceField;
      const val = item[fieldName] || '';
      row[shopifyCol] = extractKwHp(val, 'kw');
    } else if (shopifyCol === 'Metafield: custom.putere_cp [single_line_text_field]') {
      const fieldName = sourceField;
      const val = item[fieldName] || '';
      row[shopifyCol] = extractKwHp(val, 'cp');
    } else {
      // Generic mapping: copy value from dataset if exists, otherwise empty.
      row[shopifyCol] = item.hasOwnProperty(sourceField) ? item[sourceField] : '';
    }
  }
  return row;
}


/*
 * Map a single dataset record to a simplified Shopify product object.  The
 * output includes core Shopify columns such as Handle, Title, Body HTML,
 * Tags, Image Src, Variant SKU and Variant Price.  It leverages the
 * normalisation performed by normalizeItem() and augments it with
 * additional fields derived from the raw dataset (e.g. generating a
 * slugified handle and concatenating description with feature lists).
 */
function mapToShopify(item) {
  // Normalise to get clean arrays and price info.
  const base = normalizeItem(item);
  // Generate a slug for the handle: lower-case, hyphenated title + id.
  const rawTitle = (item.title || '').toString().trim();
  let slug = rawTitle
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  // Append the unique id to ensure uniqueness.
  const idStr = item.id !== undefined && item.id !== null ? String(item.id).trim() : '';
  if (slug && idStr) {
    slug = `${slug}-${idStr}`;
  } else if (idStr) {
    slug = idStr;
  }
  // Tags: combine segment and category when available.
  const tags = [item.segment, item.category]
    .filter((v) => v && String(v).trim())
    .map((v) => String(v).trim())
    .join(', ');
  // Build Body HTML from description and feature list.  We try several
  // possible fields for description: description, description_tag or
  // seller description.  If no description exists but there are
  // features, we still construct the list.
  const description = (item.description || item.description_tag || item.desc || '').toString().trim();
  let features = [];
  try {
    features = base.features_json ? JSON.parse(base.features_json) : [];
  } catch {
    features = [];
  }
  let bodyHtml = '';
  if (description) {
    bodyHtml += description;
  }
  if (features && Array.isArray(features) && features.length > 0) {
    const list = features
      .filter((f) => f && String(f).trim())
      .map((f) => `<li>${String(f).trim()}</li>`)  // escape not necessary for simple text
      .join('');
    if (list) {
      bodyHtml += bodyHtml ? '\n<ul>' + list + '</ul>' : '<ul>' + list + '</ul>';
    }
  }
  // Determine the primary image.  Prefer image_1 from normalized output,
  // fallback to previewImage or the first item in the images array.
  let imageSrc = '';
  if (base.image_1) {
    imageSrc = base.image_1;
  } else if (item.previewImage) {
    imageSrc = item.previewImage;
  } else if (Array.isArray(item.images) && item.images.length > 0) {
    const first = item.images[0];
    imageSrc = (first && typeof first === 'object') ? (first.url || first.src || '') : String(first);
  }
  // Variant price: use amount from normalized price, fallback to raw price.
  let variantPrice = '';
  if (base.price_amount) {
    variantPrice = base.price_amount;
  } else if (item.price && typeof item.price === 'object' && (item.price.amount || item.price.value)) {
    variantPrice = item.price.amount || item.price.value;
  } else if (typeof item.price === 'string') {
    const m = item.price.match(/[\d.,]+/);
    if (m) variantPrice = m[0];
  }
  // Determine vendor and type for Shopify.  Use available fields such as
  // brand/vendor and category/segment.
  const vendor = item.brand || item.vendor || item.seller || '';
  const productType = item.category || item.segment || '';
  return {
    Handle: slug || '',
    Title: base.title || rawTitle || '',
    'Body HTML': bodyHtml || '',
    Vendor: vendor || '',
    Type: productType || '',
    Tags: tags || '',
    'Image Src': imageSrc || '',
    'Variant SKU': idStr || '',
    'Variant Price': variantPrice || '',
    ...base
  };
}

/*
 * GET /api/shopify-results
 *
 * Build a Shopify-importable dataset from a completed run.  This endpoint
 * accepts the runId and returns either JSON, CSV or Excel data.  Each
 * record is mapped to include the core Shopify columns (Handle, Title,
 * Body HTML, Tags, Image Src, Variant SKU, Variant Price) along with
 * the normalised images, features and attributes fields.
 */
app.get('/api/shopify-results', async (req, res) => {
  try {
    const { runId, format = 'csv' } = req.query;
    if (!runId) {
      return res.status(400).json({ error: 'runId query parameter is required' });
    }
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    // Fetch run details to get dataset ID.
    const runResp = await fetch(`https://api.apify.com/v2/actor-runs/${runId}`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const runData = await runResp.json();
    if (!runResp.ok) {
      return res.status(runResp.status).json({ error: runData.error || 'Failed to fetch run details', detail: runData });
    }
    const datasetId = runData.data?.defaultDatasetId || runData.defaultDatasetId;
    if (!datasetId) {
      return res.status(404).json({ error: 'Dataset ID not found for this run. Make sure the run has finished successfully.' });
    }
    // Fetch dataset items as JSON.
    const itemsResp = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=json`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const jsonItems = await itemsResp.json();
    const items = Array.isArray(jsonItems) ? jsonItems : [];
    // Load metafields mapping and map each item.
    const mappingList = await loadMetafieldsMappingFile();
    const mapped = mapDatasetToMetafields(items, mappingList);
    const fmt = String(format).toLowerCase();
    if (fmt === 'json') {
      return res.json(mapped);
    } else if (fmt === 'csv') {
      const csv = toCsv(mapped);
      res.set('Content-Type', 'text/csv');
      res.set('Content-Disposition', `attachment; filename="${runId}-shopify.csv"`);
      return res.send(csv);
    } else if (fmt === 'xlsx') {
      const workbook = new ExcelJS.Workbook();
      const sheet = workbook.addWorksheet('Shopify');
      // Determine headers
      const headerSet = new Set();
      mapped.forEach((row) => {
        Object.keys(row).forEach((k) => headerSet.add(k));
      });
      const headers = Array.from(headerSet);
      sheet.addRow(headers);
      mapped.forEach((row) => {
        const line = headers.map((h) => row[h] ?? '');
        sheet.addRow(line);
      });
      const buffer = await workbook.xlsx.writeBuffer();
      res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.set('Content-Disposition', `attachment; filename="${runId}-shopify.xlsx"`);
      return res.send(Buffer.from(buffer));
    }
    return res.status(400).json({ error: 'Unsupported format. Use json, csv or xlsx.' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/*
 * GET /api/images-exploded
 *
 * Produce a long-format list of images for each product variant.  Each
 * record includes the variant SKU, handle and image link.  The variant
 * SKU is derived from the dataset's "id" field, and the handle is
 * generated in the same way as mapToShopify().  A single product with
 * multiple images will produce multiple rows.  Supported formats are
 * json, csv and xlsx.
 */
app.get('/api/images-exploded', async (req, res) => {
  try {
    const { runId, format = 'csv' } = req.query;
    if (!runId) {
      return res.status(400).json({ error: 'runId query parameter is required' });
    }
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    // Fetch run details to get dataset ID.
    const runResp = await fetch(`https://api.apify.com/v2/actor-runs/${runId}`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const runData = await runResp.json();
    if (!runResp.ok) {
      return res.status(runResp.status).json({ error: runData.error || 'Failed to fetch run details', detail: runData });
    }
    const datasetId = runData.data?.defaultDatasetId || runData.defaultDatasetId;
    if (!datasetId) {
      return res.status(404).json({ error: 'Dataset ID not found for this run. Make sure the run has finished successfully.' });
    }
    // Fetch dataset items as JSON.
    const itemsResp = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=json`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const jsonItems = await itemsResp.json();
    const items = Array.isArray(jsonItems) ? jsonItems : [];
    const records = [];
    for (const item of items) {
      const base = normalizeItem(item);
      const rawTitle = (item.title || '').toString().trim();
      let slug = rawTitle
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
      const idStr = item.id !== undefined && item.id !== null ? String(item.id).trim() : '';
      if (slug && idStr) slug = `${slug}-${idStr}`;
      else if (idStr) slug = idStr;
      // Collect image URLs.
      let images = [];
      try {
        images = base.images_json ? JSON.parse(base.images_json) : [];
      } catch {
        images = [];
      }
      // If no images found in normalised structure, check raw images array.
      if (images.length === 0 && Array.isArray(item.images)) {
        images = item.images.map((img) => {
          if (img && typeof img === 'object') return img.url || img.src || '';
          return String(img);
        });
      }
      images.forEach((link) => {
        if (link) {
          records.push({
            'Variant SKU': idStr,
            Handle: slug,
            'Image Src': link
          });
        }
      });
    }
    const fmt = format.toLowerCase();
    if (fmt === 'json') {
      return res.json(records);
    } else if (fmt === 'csv') {
      const csv = toCsv(records);
      res.set('Content-Type', 'text/csv');
      res.set('Content-Disposition', `attachment; filename="${runId}-images.csv"`);
      return res.send(csv);
    } else if (fmt === 'xlsx') {
      const workbook = new ExcelJS.Workbook();
      const sheet = workbook.addWorksheet('Images');
      const headerSet = new Set();
      records.forEach((rec) => Object.keys(rec).forEach((key) => headerSet.add(key)));
      const headers = Array.from(headerSet);
      sheet.addRow(headers);
      records.forEach((rec) => {
        const row = headers.map((h) => rec[h] ?? '');
        sheet.addRow(row);
      });
      const buffer = await workbook.xlsx.writeBuffer();
      res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.set('Content-Disposition', `attachment; filename="${runId}-images.xlsx"`);
      return res.send(Buffer.from(buffer));
    }
    return res.status(400).json({ error: 'Unsupported format. Use json, csv or xlsx.' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/*
 * Serve a simple HTML page that lets the user enter a mobile.de search
 * URL and optional maximum item count. It includes a bit of client-side
 * JavaScript to start the actor run and poll for status updates.
 */
app.get('/', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Mobile.de Data Crawler</title>
    <style>
      body { font-family: Arial, sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; }
      h1 { font-size: 24px; margin-bottom: 20px; }
      label { font-weight: bold; display: block; margin-top: 12px; }
      input[type=text], input[type=number] { width: 100%; padding: 8px; box-sizing: border-box; margin-top: 4px; }
      button { margin-top: 16px; padding: 10px 20px; font-size: 16px; }
      #progress { margin-top: 20px; font-weight: bold; }
      #links a { display: inline-block; margin-right: 12px; }
    </style>
  </head>
  <body>
    <h1>Mobile.de Data Crawler</h1>
    <form id="crawlForm">
      <label for="searchUrl">Search URL</label>
      <input type="text" id="searchUrl" name="searchUrl" placeholder="https://suchen.mobile.de/fahrzeuge/search.html?..." required />
      <label for="maxItems">Maximum items (optional)</label>
      <input type="number" id="maxItems" name="maxItems" min="1" placeholder="e.g. 50" />
      <button type="submit">Start crawl</button>
    </form>
    <div id="progress"></div>
    <div id="links"></div>
    <script>
      document.getElementById('crawlForm').addEventListener('submit', async function (e) {
        e.preventDefault();
        const searchUrl = document.getElementById('searchUrl').value.trim();
        const maxItems = document.getElementById('maxItems').value;
        document.getElementById('progress').textContent = '';
        document.getElementById('links').innerHTML = '';
        if (!searchUrl) {
          alert('Please provide a search URL');
          return;
        }
        try {
          const resp = await fetch('/api/start-run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ searchUrl, maxItems })
          });
          const json = await resp.json();
          if (json.error) {
            alert(json.error);
            return;
          }
          const runId = json.runId;
          document.getElementById('progress').textContent = 'Run started. ID: ' + runId;
          // Poll for status every 3 seconds.
          const poll = setInterval(async () => {
            try {
              const statusResp = await fetch('/api/run-status?runId=' + encodeURIComponent(runId));
              const status = await statusResp.json();
              const succeededCount = status.stats && (status.stats.succeeded || status.stats.itemsOutput) || 0;
              document.getElementById('progress').textContent = 'Status: ' + status.status + ' | Succeeded: ' + succeededCount;
              if (status.status === 'SUCCEEDED') {
                clearInterval(poll);
                document.getElementById('progress').textContent = 'Run succeeded. ' + succeededCount + ' items.';
                document.getElementById('links').innerHTML =
                  '<p>Downloads:</p>' +
                  '<a href="/api/run-results?runId=' + runId + '&format=json">Raw JSON</a> | ' +
                  '<a href="/api/run-results?runId=' + runId + '&format=csv">Raw CSV</a> | ' +
                  '<a href="/api/run-results?runId=' + runId + '&format=xlsx">Raw Excel</a> | ' +
                  '<a href="/api/run-results?runId=' + runId + '&format=json&normalized=true">Normalized JSON</a> | ' +
                  '<a href="/api/run-results?runId=' + runId + '&format=csv&normalized=true">Normalized CSV</a> | ' +
                  '<a href="/api/run-results?runId=' + runId + '&format=xlsx&normalized=true">Normalized Excel</a>';
              } else if (status.status === 'FAILED' || status.status === 'ABORTED') {
                clearInterval(poll);
                document.getElementById('progress').textContent = 'Run ended with status: ' + status.status;
              }
            } catch (err) {
              clearInterval(poll);
              document.getElementById('progress').textContent = 'Error while polling run status';
            }
          }, 3000);
        } catch (err) {
          alert('Failed to start run: ' + err.message);
        }
      });
    </script>
  </body>
</html>`);
});

/*
 * POST /api/start-run
 *
 * Kick off an Apify actor run asynchronously. Accepts JSON body with
 * "searchUrl" and optional "maxItems". Returns the ID of the run. If
 * APIFY_USE_ACTOR is false, this endpoint is disabled to prevent
 * accidental misuse.
 */
app.post('/api/start-run', async (req, res) => {
  try {
    const { searchUrl, maxItems } = req.body || {};
    if (!searchUrl) {
      return res.status(400).json({ error: 'searchUrl is required' });
    }
    // Determine which token to use. Prefer APIFY_TOKEN, fallback to APIFY_API_TOKEN.
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    const useActor = String(process.env.APIFY_USE_ACTOR || 'true').toLowerCase() !== 'false';
    if (!useActor) {
      return res.status(400).json({ error: 'APIFY_USE_ACTOR=false. Cannot start actor run.' });
    }
    const actorId = process.env.APIFY_ACTOR_ID;
    if (!actorId) {
      return res.status(500).json({ error: 'APIFY_ACTOR_ID must be set' });
    }
    // Build actor input. The mobile.de scraper expects an object with start_urls
    // containing the search URL and an optional max_items property.
    const input = {
      start_urls: [{ url: searchUrl }]
    };
    const max = parseInt(maxItems);
    if (!Number.isNaN(max) && max > 0) input.max_items = max;
    // Start the actor run asynchronously.
    const runResp = await fetch(`https://api.apify.com/v2/acts/${actorId}/runs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({ input })
    });
    const runData = await runResp.json();
    if (!runResp.ok) {
      return res.status(runResp.status).json({ error: runData.error || 'Failed to start actor run', detail: runData });
    }
    const runId = runData.data?.id || runData.id;
    if (!runId) {
      return res.status(500).json({ error: 'Actor run did not return an ID', detail: runData });
    }
    return res.json({ runId });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/*
 * GET /api/run-status
 *
 * Poll the status of a running actor. Requires "runId" as a query
 * parameter. Returns the Apify run JSON as-is. If the run is not found or
 * there is an API error, it will return the error from Apify.
 */
app.get('/api/run-status', async (req, res) => {
  try {
    const { runId } = req.query;
    if (!runId) {
      return res.status(400).json({ error: 'runId query parameter is required' });
    }
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    const statusResp = await fetch(`https://api.apify.com/v2/actor-runs/${runId}`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const statusData = await statusResp.json();
    if (!statusResp.ok) {
      return res.status(statusResp.status).json({ error: statusData.error || 'Failed to fetch run status', detail: statusData });
    }
    return res.json(statusData.data || statusData);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/*
 * GET /api/run-results
 *
 * Fetch items from a completed actor run's dataset. Accepts the following
 * query parameters:
 *   runId      – required. The ID of the Apify run.
 *   format     – one of "json", "csv" or "xlsx". Defaults to "json".
 *   normalized – if "true", apply the normalizeItem() transformation to
 *                each record. Defaults to false.
 *
 * The endpoint will determine the run's defaultDatasetId, fetch the items
 * from the dataset in JSON, optionally normalise them and then return
 * either JSON, CSV or Excel content. Excel files are built on-the-fly
 * using exceljs.
 */
app.get('/api/run-results', async (req, res) => {
  try {
    const { runId, format = 'json', normalized } = req.query;
    if (!runId) {
      return res.status(400).json({ error: 'runId query parameter is required' });
    }
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    // Fetch run to get defaultDatasetId and status.
    const runResp = await fetch(`https://api.apify.com/v2/actor-runs/${runId}`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const runData = await runResp.json();
    if (!runResp.ok) {
      return res.status(runResp.status).json({ error: runData.error || 'Failed to fetch run details', detail: runData });
    }
    const datasetId = runData.data?.defaultDatasetId || runData.defaultDatasetId;
    if (!datasetId) {
      return res.status(404).json({ error: 'Dataset ID not found for this run. Make sure the run has finished successfully.' });
    }
    // Determine whether normalisation is requested.
    const doNormalize = String(normalized || 'false').toLowerCase() === 'true';
    // Always fetch JSON when normalisation is requested or when the desired format is xlsx.
    let items = [];
    if (doNormalize || format === 'xlsx') {
      const itemsResp = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=json`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const jsonItems = await itemsResp.json();
      items = Array.isArray(jsonItems) ? jsonItems : [];
      if (doNormalize) {
        items = items.map(normalizeItem);
      }
    }
    // Based on the requested format, return the data.
    const fmt = format.toLowerCase();
    if (fmt === 'json') {
      // If normalization requested and items loaded above, return them; otherwise fetch raw JSON.
      if (!doNormalize) {
        const rawResp = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=json`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });
        const rawJson = await rawResp.json();
        return res.json(rawJson);
      }
      return res.json(items);
    } else if (fmt === 'csv') {
      if (!doNormalize) {
        // Streaming CSV directly from Apify for raw dataset.
        const csvResp = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=csv`, {
          headers: { 'Authorization': `Bearer ${token}` }
        });
        const csvBody = await csvResp.text();
        res.set('Content-Type', 'text/csv');
        res.set('Content-Disposition', `attachment; filename="${runId}-items.csv"`);
        return res.send(csvBody);
      }
      // Normalised CSV: convert normalised items to CSV via toCsv().
      const csv = toCsv(items);
      res.set('Content-Type', 'text/csv');
      res.set('Content-Disposition', `attachment; filename="${runId}-normalized.csv"`);
      return res.send(csv);
    } else if (fmt === 'xlsx') {
      // Build an Excel workbook from the items (either normalised or raw JSON).
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Sheet1');
      // Determine all headers.
      const headerSet = new Set();
      items.forEach((row) => {
        Object.keys(row).forEach((key) => headerSet.add(key));
      });
      const headers = Array.from(headerSet);
      worksheet.addRow(headers);
      items.forEach((row) => {
        const line = headers.map((h) => row[h] ?? '');
        worksheet.addRow(line);
      });
      const buffer = await workbook.xlsx.writeBuffer();
      res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.set('Content-Disposition', `attachment; filename="${runId}-${doNormalize ? 'normalized' : 'items'}.xlsx"`);
      return res.send(Buffer.from(buffer));
    }
    return res.status(400).json({ error: 'Unsupported format. Use json, csv or xlsx.' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/*
 * GET /api/shopify-import
 *
 * Generate a Shopify import file based on the completed actor run's dataset
 * and a metafields mapping sheet.  This endpoint accepts:
 *   runId  – required. The ID of the Apify run.
 *   format – one of 'json', 'csv' or 'xlsx'. Defaults to 'json'.
 *
 * It loads the mapping file (sheet 'Metafields_Mapping') from
 * dataset_mobile-de-scraper_mapped_metafields.xlsx (or from the file
 * specified via METAFIELDS_MAPPING_FILE) and applies it to each
 * dataset item to produce a structured Shopify import row.  Additional
 * columns such as Title, Variant SKU/ID, Vendor, Image Src, Tags,
 * Body HTML and Features are generated automatically.  The resulting
 * dataset can be downloaded in JSON, CSV or Excel formats.
 */
app.get('/api/shopify-import', async (req, res) => {
  try {
    const { runId, format = 'json' } = req.query;
    if (!runId) {
      return res.status(400).json({ error: 'runId query parameter is required' });
    }
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    // Fetch run details to get dataset ID.
    const runResp = await fetch(`https://api.apify.com/v2/actor-runs/${runId}`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const runData = await runResp.json();
    if (!runResp.ok) {
      return res.status(runResp.status).json({ error: runData.error || 'Failed to fetch run details', detail: runData });
    }
    const datasetId = runData.data?.defaultDatasetId || runData.defaultDatasetId;
    if (!datasetId) {
      return res.status(404).json({ error: 'Dataset ID not found for this run. Ensure the run has finished.' });
    }
    // Load raw items from the dataset.
    const itemsResp = await fetch(`https://api.apify.com/v2/datasets/${datasetId}/items?clean=true&format=json`, {
      headers: { 'Authorization': `Bearer ${token}` }
    });
    const rawItems = await itemsResp.json();
    const items = Array.isArray(rawItems) ? rawItems : [];
    // Load metafields mapping
    const mappingList = await loadMetafieldsMappingFile();
    // Map items to metafield rows
    const mapped = mapDatasetToMetafields(items, mappingList);
    const fmt = String(format).toLowerCase();
    if (fmt === 'json') {
      return res.json(mapped);
    } else if (fmt === 'csv') {
      const csv = toCsv(mapped);
      res.set('Content-Type', 'text/csv');
      res.set('Content-Disposition', `attachment; filename="${runId}-shopify-import.csv"`);
      return res.send(csv);
    } else if (fmt === 'xlsx') {
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('ShopifyImport');
      // Determine all headers in order of appearance from mapping list
      const headerSet = new Set();
      mapped.forEach((row) => {
        Object.keys(row).forEach((key) => headerSet.add(key));
      });
      const headers = Array.from(headerSet);
      worksheet.addRow(headers);
      mapped.forEach((row) => {
        const line = headers.map((h) => row[h] ?? '');
        worksheet.addRow(line);
      });
      const buffer = await workbook.xlsx.writeBuffer();
      res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.set('Content-Disposition', `attachment; filename="${runId}-shopify-import.xlsx"`);
      return res.send(Buffer.from(buffer));
    }
    return res.status(400).json({ error: 'Unsupported format. Use json, csv or xlsx.' });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/*
 * Start the Express server. Log a message when ready.
 */
app.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`);
});