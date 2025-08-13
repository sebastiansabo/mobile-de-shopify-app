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

// Translation dictionaries for attribute names and feature strings into Romanian.
// Only a subset of common keys are included; any missing keys or features
// will default to their original English names.
const ATTR_TRANSLATIONS = {
  'Vehicle condition': 'Stare vehicul',
  'Category': 'Categorie',
  'Model range': 'Gamă model',
  'Trim line': 'Nivel echipare',
  'Vehicle Number': 'Număr vehicul',
  'Availability': 'Disponibilitate',
  'Origin': 'Origine',
  'Mileage': 'Kilometraj',
  'Cubic Capacity': 'Capacitate cilindrică',
  'Power': 'Putere',
  'Drive type': 'Tip tracțiune',
  'Fuel': 'Combustibil',
  'Number of Seats': 'Număr de locuri',
  'Door Count': 'Număr uși',
  'Sliding door': 'Ușă culisantă',
  'Transmission': 'Transmisie',
  'Emission Class': 'Clasă de emisii',
  'Emissions Sticker': 'Etichetă emisii',
  'First Registration': 'Prima înmatriculare',
  'Number of Vehicle Owners': 'Număr de proprietari',
  'HU': 'ITP',
  'Climatisation': 'Climatizare',
  'Parking sensors': 'Senzori parcare',
  'Airbags': 'Airbaguri',
  'Colour (Manufacturer)': 'Culoare (producător)',
  'Colour': 'Culoare',
  'Interior Design': 'Design interior',
  'Energy consumption (comb.)': 'Consum de energie (comb.)',
  'CO₂ emissions (comb.)': 'Emisii CO₂ (comb.)',
  'Trailer load braked': 'Sarcină remorcă frânată',
  'Trailer load unbraked': 'Sarcină remorcă nefrânată',
  'Weight': 'Greutate',
  'Last service (mileage)': 'Ultimul service (kilometraj)',
  'Cylinders': 'Cilindri',
  'Tank capacity': 'Capacitate rezervor'
};

const FEATURE_TRANSLATIONS = {
  'ABS': 'ABS',
  'Adaptive Cruise Control': 'Control adaptiv al vitezei',
  'Adaptive lighting': 'Iluminare adaptivă',
  'Alarm system': 'Sistem de alarmă',
  'Alloy wheels': 'Jante din aliaj',
  'Arm rest': 'Cotieră',
  'Bi-xenon headlights': 'Faruri bi-xenon',
  'Bluetooth': 'Bluetooth',
  'Cargo barrier': 'Barieră pentru bagaje',
  'CD player': 'CD player',
  'Central locking': 'Închidere centralizată',
  'Cruise control': 'Pilot automat',
  'Distance warning system': 'Sistem avertizare distanță',
  'Electric seat adjustment': 'Reglaj electric scaune',
  'Electric windows': 'Geamuri electrice',
  'Emergency brake assist': 'Asistență frânare de urgență',
  'ESP': 'ESP',
  'Front wheel drive': 'Tracțiune față',
  'Hands-free kit': 'Set mâini libere',
  'Headlight washer system': 'Sistem spălare faruri',
  'Heated seats': 'Scaune încălzite',
  'Hill-start assist': 'Asistență la pornirea în rampă',
  'Immobilizer': 'Imobilizator',
  'Isofix': 'Isofix',
  'Leather steering wheel': 'Volan îmbrăcat în piele',
  'LED running lights': 'Lumini de zi LED',
  'Light sensor': 'Senzor lumină',
  'Lumbar support': 'Suport lombar',
  'Massage seats': 'Scaune masaj',
  'Multifunction steering wheel': 'Volan multifuncțional',
  'Navigation system': 'Sistem de navigație',
  'On-board computer': 'Computer de bord',
  'Panoramic roof': 'Plafon panoramic',
  'Passenger seat Isofix point': 'Punct Isofix pentru scaun pasager',
  'Power Steering': 'Servodirecție',
  'Rain sensor': 'Senzor de ploaie',
  'Roof rack': 'Portbagaj pe acoperiș',
  'Ski bag': 'Sac pentru schiuri',
  'Sound system': 'Sistem audio',
  'Speed limit control system': 'Sistem control limită viteză',
  'Sunroof': 'Trapă',
  'Tinted windows': 'Geamuri fumurii',
  'Touchscreen': 'Ecran tactil',
  'Traction control': 'Control tracțiune',
  'Traffic sign recognition': 'Recunoaștere indicatoare',
  'Tuner/radio': 'Radio',
  'Tyre pressure monitoring': 'Monitorizare presiune anvelope',
  'USB port': 'Port USB',
  'Winter package': 'Pachet de iarnă'
};

// Translation dictionary for common attribute values into Romanian.  Only
// selected values are translated; any values not present will be left
// unchanged.  When values contain multiple parts separated by commas,
// only exact matches will be translated.
const VALUE_TRANSLATIONS = {
  'Used vehicle': 'Vehicul folosit',
  'Accident-free': 'Fără accident',
  'Saloon': 'Sedan',
  'Cabriolet / Roadster': 'Cabriolet / Roadster',
  'Estate car': 'Break',
  'Van / Minibus': 'Van / Microbuz',
  'Now': 'Acum',
  'German edition': 'Ediție germană',
  'Internal combustion engine': 'Motor cu combustie internă',
  'Petrol': 'Benzină',
  'Diesel': 'Motorină',
  'Petrol, E10-enabled': 'Benzină, compatibil E10',
  'Automatic': 'Automată',
  'Manual gearbox': 'Cutie manuală',
  'Manual': 'Manuală',
  'Automatic climatisation, 2 zones': 'Climatizare automată, 2 zone',
  'Automatic climatisation, 3 zones': 'Climatizare automată, 3 zone',
  'Automatic air conditioning': 'Aer condiționat automat',
  'Rear, Front': 'Spate, Față',
  'Rear, Camera, Front': 'Spate, Cameră, Față',
  'Driver Airbag': 'Airbag șofer',
  'Front and Side Airbags': 'Airbaguri frontale și laterale',
  'Front and Side and More Airbags': 'Airbaguri frontale, laterale și altele',
  'Pure White': 'Alb Pur',
  'Black Metallic': 'Negru Metalic',
  'Brown Metallic': 'Maro Metalic',
  'Blue': 'Albastru',
  'Red': 'Roșu',
  'Cloth, Black': 'Textil, Negru',
  'Cloth, Brown': 'Textil, Maro',
  'Full leather, Beige': 'Piele integrală, Bej',
  'Full leather, Brown': 'Piele integrală, Maro',
  'Full leather, Other': 'Piele integrală, Alte',
  'Euro5': 'Euro5',
  'Euro6': 'Euro6',
  '4 (Green)': '4 (Verde)',
  'New': 'Nou',
  'Used': 'Folosit'
  ,
  // Transmission and fuel values
  'Manual': 'Manuală',
  'Manual gearbox': 'Manuală',
  'Automatic': 'Automată',
  'Automatic gearbox': 'Automată',
  // Fuel types
  'Petrol': 'Benzină',
  'Diesel': 'Diesel',
  // Colours
  'Brown': 'Maro',
  'White': 'Alb',
  'Black': 'Negru'
  ,
  // Additional value translations for common fields
  // Some attribute values such as drive type and parking sensor positions
  // appear as single words.  Provide Romanian equivalents where
  // possible.  Values not listed here will fall back to the original.
  'Internal': 'Intern',
  'Front': 'Față',
  'Rear': 'Spate',
  'Camera': 'Cameră'
};

// Define a mapping from current output field names to new metafield names.  This
// mapping is used to build the final Shopify export in a two‑step flow
// (Step 1 raw data, Step 2 normalized/mapped data).  Only keys present
// here or in the list of always‑kept fields will be included in the
// output.  Values not found in this mapping will be omitted from the
// Shopify export.
const NEW_METAFIELD_MAPPINGS = {
  // Simple renames
  'dealerDetails': 'Vendor',
  'Număr vehicul': 'Variant Barcode',
  'Greutate': 'Variant Weight',
  // Dataset Vendor field maps to a custom metafield for dossier number
  'Vendor': 'Metafield: custom.nr_dos_ [single_line_text_field]',
  'Prima înmatriculare': 'Metafield: custom.data_livrarii [single_line_text_field]',
  'Capacitate cilindrică': 'Metafield: custom.cilindree [single_line_text_field]',
  'Emisii CO₂ (comb.)': 'Metafield: custom.emisii_co2 [single_line_text_field]',
  'Transmisie': 'Metafield: custom.cutie_viteze [single_line_text_field]',
  'Clasă de emisii': 'Metafield: custom.clasa_de_emisii_noxe [single_line_text_field]',
  'Car Url': 'Metafield: custom.nr_imatr_ [single_line_text_field]',
  'Culoare': 'Metafield: custom.culoare [single_line_text_field]',
  'Număr uși': 'Metafield: custom.nr_de_usi [single_line_text_field]',
  'Construction Year': 'Metafield: custom.anul_modelului [single_line_text_field]',
  'Putere': 'Metafield: custom.putere_cp [single_line_text_field]',
  'Kilometraj': 'Metafield: custom.kilometraj [single_line_text_field]',
  'Nivel echipare': 'Metafield: custom.nivel_de_echipare [single_line_text_field]',
  'Features': 'Metafield: custom.dotari [multi_line_text_field]',
  'Categorie': 'Metafield: custom.bodu_type [single_line_text_field]',
  'Vehicle tax': 'Metafield: custom.tva [single_line_text_field]',
  'Combustibil': 'Metafield: custom.fuel [single_line_text_field]',
  'Battery capacity (in kWh)': 'Metafield: custom.capacitate_baterie [single_line_text_field]',
  'Electric range (EAER)': 'Metafield: custom.range_mod_electric [single_line_text_field]',
  'Consum de energie (comb.)': 'Metafield: custom.consum_combinat [single_line_text_field]',
  'price/withoutVAT/amount': 'Metafield: custom.pret_furnizor [single_line_text_field]'
};

// List of core fields that should always be kept unchanged in the final
// Shopify export.  These include core Shopify columns and metafields
// already named in the desired format.  Fields not in this list or in
// NEW_METAFIELD_MAPPINGS will be omitted.
const ALWAYS_KEEP_FIELDS = new Set([
  'Title',
  'Body HTML',
  'Tags',
  'Image Src',
  'Image Alt Text',
  'Variant SKU',
  'Variant Price',
  'Metafield: custom.model [single_line_text_field]',
  'Metafield: custom.marca [single_line_text_field]',
  'Metafield: custom.putere_kw [single_line_text_field]',
  'Metafield: custom.putere_cp [single_line_text_field]',
  'Metafield: custom.transmisie [list.single_line_text_field]'
  ,
  // Preserve custom TVA metafield generated by buildMetafieldsRow
  'Metafield: custom.tva [single_line_text_field]'
  ,
  // Keep the product handle used by Shopify to generate URLs
  'Handle'
  ,
  // Keep the template suffix column used for Shopify theme selection
  'Template Suffix'
]);

/**
 * Transform a row produced by buildMetafieldsRow() into a row using the
 * new metafield names.  It renames keys according to
 * NEW_METAFIELD_MAPPINGS and drops any keys not listed in
 * NEW_METAFIELD_MAPPINGS or ALWAYS_KEEP_FIELDS.  Keys listed in
 * ALWAYS_KEEP_FIELDS are kept as‑is.  Keys in NEW_METAFIELD_MAPPINGS
 * are renamed to their new names.  All other keys are omitted.
 *
 * @param {Object} row The original row from buildMetafieldsRow.
 * @returns {Object} A new row with renamed and filtered keys.
 */
function mapToNewMetafields(row) {
  const newRow = {};
  for (const key of Object.keys(row)) {
    if (ALWAYS_KEEP_FIELDS.has(key)) {
      newRow[key] = row[key];
    } else if (NEW_METAFIELD_MAPPINGS[key]) {
      const newKey = NEW_METAFIELD_MAPPINGS[key];
      newRow[newKey] = row[key];
    }
    // omit everything else
  }
  return newRow;
}


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
    const rawName = attr.name ? String(attr.name).trim() : '';
    if (!rawName) return;
    // Translate attribute name to Romanian if available for column headers.
    const name = ATTR_TRANSLATIONS[rawName] || rawName;
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
    // Translate the short value into Romanian if a translation exists.
    // Certain attributes such as colour should retain their original
    // values.  When the attribute name corresponds to Colour or
    // Colour (Manufacturer) we skip translating the value.
    let translatedVal = shortVal;
    const rawKey = rawName;
    const skipValueTranslation =
      rawKey === 'Colour' || rawKey === 'Colour (Manufacturer)';
    if (!skipValueTranslation && VALUE_TRANSLATIONS.hasOwnProperty(shortVal)) {
      translatedVal = VALUE_TRANSLATIONS[shortVal];
    }
    // Assign the translated (or original) short value to the column if not already set.
    if (!Object.prototype.hasOwnProperty.call(cols, name)) {
      cols[name] = translatedVal;
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
  // Handle: generate a slug from the title and ID.  The handle is
  // lowercased, contains only alphanumerics and hyphens, and ends
  // with the item ID to ensure uniqueness.  This mirrors Shopify’s
  // handle generation logic used in mapItemToShopifyTemplate().
  {
    const rawTitle = (item && item.title ? String(item.title) : '').trim();
    let slug = rawTitle
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
    const idStr = item && item.id !== undefined && item.id !== null ? String(item.id).trim() : '';
    if (slug && idStr) slug = `${slug}-${idStr}`;
    else if (idStr) slug = idStr;
    row['Handle'] = slug;
  }
  // Variant SKU / ID
  const idVal = item && item.id !== undefined ? String(item.id) : '';
  row['Variant SKU'] = idVal;
  row['Variant ID'] = idVal;
  // Variant Price: calculate from the price amount according to the business rule.
  // The variant price should equal price.amount * 1.21 / 1.19.  We
  // attempt to parse the numeric amount from the price object or
  // related fields.  Commas are treated as decimal separators and
  // spaces are removed.  If a valid number cannot be parsed, we
  // leave the variant price untouched (either existing mapping or
  // blank).
  {
    // Determine the source amount string from the item.  Prefer
    // item.price.amount, then item.price.total.amount, then
    // item.price.value, then price/total/amount field.
    let amountStr = '';
    if (item && item.price && typeof item.price === 'object' && item.price !== null) {
      if (item.price.amount) amountStr = String(item.price.amount);
      else if (item.price.total && item.price.total.amount) amountStr = String(item.price.total.amount);
      else if (item.price.value) amountStr = String(item.price.value);
    }
    if (!amountStr && item && item['price/total/amount']) {
      amountStr = String(item['price/total/amount']);
    }
    // Clean the amount: remove currency symbols and spaces.
    let numVal = NaN;
    if (amountStr) {
      // Replace comma with dot for decimal, remove thousands separators (dots or commas) and spaces.
      // e.g. "15,873.11" -> "15873.11", "15873,11" -> "15873.11".
      let cleaned = amountStr
        .replace(/\s+/g, '')
        .replace(/[€£$]/g, '')
        .trim();
      // If the string has both comma and dot, assume comma is thousands sep; remove commas.
      if (cleaned.includes(',') && cleaned.includes('.')) {
        cleaned = cleaned.replace(/,/g, '');
      }
      // If only comma exists, treat it as decimal separator.
      if (!cleaned.includes('.') && cleaned.includes(',')) {
        cleaned = cleaned.replace(/,/g, '.');
      }
      // Remove any non-digit/dot characters (e.g. "km", "ccm", etc.) at end
      cleaned = cleaned.match(/[0-9.]+/)
        ? cleaned.match(/[0-9.]+/)[0]
        : cleaned;
      numVal = parseFloat(cleaned);
    }
    if (!isNaN(numVal) && numVal > 0) {
      // First compute price adjusted for VAT difference
      let priceWithVatAdj = (numVal * 1.21) / 1.19;
      // Determine extra commission based on original amount before VAT adjustment.
      // Commission thresholds are applied on the raw amount (numVal) per the user specification.
      let commissionRate = 0;
      const amt = numVal;
      if (amt < 25000) commissionRate = 0.095; // <25k → 9.50%
      else if (amt < 40000) commissionRate = 0.075; // 25k–<40k → 7.50%
      else if (amt < 60000) commissionRate = 0.065; // 40k–<60k → 6.50%
      else if (amt < 90000) commissionRate = 0.055; // 60k–<90k → 5.50%
      else if (amt < 250000) commissionRate = 0.045; // 90–<250k → 4.50%
      else commissionRate = 0.035; // ≥250k → 3.50%
      // Apply commission
      priceWithVatAdj = priceWithVatAdj * (1 + commissionRate);
      // Round to two decimals
      const rounded = Math.round(priceWithVatAdj * 100) / 100;
      row['Variant Price'] = rounded.toString();
    } else {
      // If no number could be parsed and the row already has a Variant Price from mapping
      // or previous logic, leave it as-is. Otherwise leave blank.
      if (!row['Variant Price']) {
        row['Variant Price'] = '';
      }
    }
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
  // Body HTML: build a unified HTML description combining attributes and features.
  // First, gather attribute entries from item.attributes.  Attributes may be
  // provided as an object or a JSON string; values may be arrays or strings.
  let attrEntries = [];
  if (item && item.attributes) {
    let attrObj = null;
    if (typeof item.attributes === 'object' && !Array.isArray(item.attributes)) {
      attrObj = item.attributes;
    } else if (typeof item.attributes === 'string') {
      try {
        const parsedAttr = JSON.parse(item.attributes);
        if (parsedAttr && typeof parsedAttr === 'object' && !Array.isArray(parsedAttr)) {
          attrObj = parsedAttr;
        }
      } catch (e) {
        // ignore parse errors
      }
    }
    if (attrObj) {
      for (const [key, val] of Object.entries(attrObj)) {
        // Use full value; if array, join with comma.  Translate values
        // into Romanian if a translation exists.  If the value is an
        // array, translate each element individually.
        let text = '';
        // Determine if this attribute's value should be translated.  Colour
        // attributes should retain their original values.
        const skipTranslationForValue =
          key === 'Colour' || key === 'Colour (Manufacturer)';
        if (Array.isArray(val)) {
          text = val
            .map((v) => {
              const sv = v === undefined || v === null ? '' : String(v).trim();
              if (!skipTranslationForValue) {
                const tr = VALUE_TRANSLATIONS[sv];
                if (tr) return tr;
              }
              return sv;
            })
            .join(', ');
        } else {
          const sv = val === undefined || val === null ? '' : String(val).trim();
          // Only translate if not excluded and an exact translation exists
          const tr = !skipTranslationForValue ? VALUE_TRANSLATIONS[sv] : undefined;
          if (tr) {
            text = tr;
          } else {
            // If the value contains commas, attempt to translate each part separately.
            if (!skipTranslationForValue && sv.includes(',')) {
              text = sv
                .split(',')
                .map((p) => {
                  const part = p.trim();
                  const trPart = VALUE_TRANSLATIONS[part];
                  return trPart || part;
                })
                .join(', ');
            } else {
              text = sv;
            }
          }
        }
        // Translate attribute name to Romanian if available.
        const roKey = ATTR_TRANSLATIONS[key] || key;
        attrEntries.push(`<li><strong>${roKey}:</strong> ${text}</li>`);
      }
    }
  }
  // Next, gather feature entries from item.features.  Features may be an
  // array of strings or objects.  Convert each to a list item.
  let featEntries = [];
  if (item && item.features) {
    const feats = safeParseMaybeList(item.features).map((f) => {
      if (f && typeof f === 'object') return f.name || f.title || '';
      return String(f);
    }).filter(Boolean);
    featEntries = feats.map((f) => {
      const roFeat = FEATURE_TRANSLATIONS[f] || f;
      return `<li>${roFeat}</li>`;
    });
  }
  // Build the Body HTML by separating attributes and features.  The
  // attributes (technical data) are labelled "Date tehnice" and the
  // features (equipment) are labelled "Dotari".  If both sections
  // exist we separate them with an <hr> tag.  Each list entry has
  // already been translated and wrapped in <li> tags.
  let bodyHtmlParts = [];
  if (attrEntries.length > 0) {
    bodyHtmlParts.push(`<p><strong>Date tehnice:</strong></p><ul>${attrEntries.join('')}</ul>`);
  }
  if (featEntries.length > 0) {
    bodyHtmlParts.push(`<p><strong>Dotari:</strong></p><ul>${featEntries.join('')}</ul>`);
  }
  if (bodyHtmlParts.length === 2) {
    row['Body HTML'] = `${bodyHtmlParts[0]}<hr>${bodyHtmlParts[1]}`;
  } else if (bodyHtmlParts.length === 1) {
    row['Body HTML'] = bodyHtmlParts[0];
  } else {
    row['Body HTML'] = '';
  }
  // Build a separate Features column containing only the equipment list.
  row['Features'] = featEntries.length > 0 ? `<ul>${featEntries.join('')}</ul>` : '';
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
  // Additional columns: extract dealer name and car URL.  dealerDetails
  // may be an object or a JSON string with a 'name' property.  The URL
  // is taken directly from the dataset.
  let dealerName = '';
  if (item && item.dealerDetails) {
    if (typeof item.dealerDetails === 'object' && item.dealerDetails !== null) {
      dealerName = item.dealerDetails.name || '';
    } else if (typeof item.dealerDetails === 'string') {
      try {
        const parsedDealer = JSON.parse(item.dealerDetails);
        if (parsedDealer && typeof parsedDealer === 'object') {
          dealerName = parsedDealer.name || '';
        }
      } catch (e) {
        // ignore parse errors
      }
    }
  }
  row['dealerDetails'] = dealerName;
  row['Car Url'] = item && item.url ? String(item.url) : '';
  // Remove unwanted columns from the final row.  'Variant ID' and 'ITP'
  // should not appear in the exported file.
  if (row.hasOwnProperty('Variant ID')) delete row['Variant ID'];
  if (row.hasOwnProperty('ITP')) delete row['ITP'];

  // Add price without VAT column.  Extract the 'price/withoutVAT/amount'
  // from the nested price object if available, or use the dot-notated
  // property from the dataset.  This column preserves the original
  // amount before VAT and provides visibility into the net price.
  let priceNoVat = '';
  if (item) {
    // Check nested object structure first
    if (
      typeof item.price === 'object' &&
      item.price !== null &&
      item.price.withoutVAT &&
      typeof item.price.withoutVAT === 'object'
    ) {
      priceNoVat = item.price.withoutVAT.amount || '';
    }
    // Fallback to dot-notated property
    if (!priceNoVat && Object.prototype.hasOwnProperty.call(item, 'price/withoutVAT/amount')) {
      priceNoVat = item['price/withoutVAT/amount'];
    }
  }
  row['price/withoutVAT/amount'] = priceNoVat || '';

  // Extract power values from attributes.  If the raw attributes
  // include a "Power" entry like "110 kW (150 hp)", split it into
  // separate fields with units for kW and hp.  These will be stored
  // under "Metafield: custom.putere_kw [single_line_text_field]" and
  // "Metafield: custom.putere_cp [single_line_text_field]".  If the
  // attribute does not exist or does not match the expected pattern,
  // the fields will remain empty.  This logic supplements any
  // existing mapping and ensures the power values are always
  // available even if the mapping file does not define these fields.
  let powerKw = '';
  let powerHp = '';
  if (item && item.attributes) {
    let powerVal;
    // attributes may be an object or a JSON string
    if (typeof item.attributes === 'object' && !Array.isArray(item.attributes)) {
      powerVal = item.attributes['Power'];
    } else if (typeof item.attributes === 'string') {
      try {
        const parsedAttr = JSON.parse(item.attributes);
        if (parsedAttr && typeof parsedAttr === 'object') {
          powerVal = parsedAttr['Power'];
        }
      } catch (e) {
        // ignore parse errors
      }
    }
    if (powerVal) {
      const s = String(powerVal);
      // Match first number + optional decimal, spaces and kW or KW
      const kwMatch = s.match(/\b(\d+[\d.,]*)\s*kW/i);
      // Match number + optional decimal and hp within parentheses
      const hpMatch = s.match(/\((\d+[\d.,]*)\s*hp\)/i);
      if (kwMatch) {
        // Preserve original unit
        powerKw = `${kwMatch[1].replace(/\s+/g, '')} kW`;
      }
      if (hpMatch) {
        powerHp = `${hpMatch[1].replace(/\s+/g, '')} hp`;
      }
    }
  }
  // Assign power values to new metafield columns if available.
  row['Metafield: custom.putere_kw [single_line_text_field]'] = powerKw;
  row['Metafield: custom.putere_cp [single_line_text_field]'] = powerHp;

  // Derive drive train metafield (custom.transmisie).  The dataset
  // stores drive type information primarily in the features list.  We
  // scan the features array for specific phrases indicating the drive
  // configuration and map them to their Romanian shorthand codes:
  //   - "Rear wheel drive"  → "4x2 (RWD)"
  //   - "Front wheel drive" → "2x4 (FWD)"
  //   - "Four-wheel drive"  → "4x4 (AWD)"
  // If multiple drive types are present, the first match wins.  If
  // none match, the column will be left empty.  The result is
  // assigned to the metafield column name specified by the user.
  let driveCode = '';
  if (item) {
    // Collect features from both the unified features array and any
    // enumerated feature keys (features/0, features/1, etc.).
    let allFeatures = [];
    // 1) Parse the top-level `features` field if present.
    if (item.features) {
      try {
        const fs = safeParseMaybeList(item.features);
        fs.forEach((f) => {
          if (f && typeof f === 'object' && (f.name || f.title)) {
            allFeatures.push(String(f.name || f.title));
          } else if (f) {
            allFeatures.push(String(f));
          }
        });
      } catch (e) {
        // ignore parse errors
      }
    }
    // 2) Parse enumerated feature fields (features/0..features/99)
    for (let i = 0; i < 100; i++) {
      const key = `features/${i}`;
      if (Object.prototype.hasOwnProperty.call(item, key)) {
        const val = item[key];
        if (val) {
          allFeatures.push(String(val));
        }
      }
    }
    // Normalize feature strings and search for drive patterns.
    for (const feat of allFeatures) {
      const txt = feat.trim().toLowerCase();
      if (txt === 'rear wheel drive') {
        driveCode = '4x2 (RWD)';
        break;
      }
      if (txt === 'front wheel drive') {
        driveCode = '2x4 (FWD)';
        break;
      }
      if (txt === 'four-wheel drive' || txt === 'four wheel drive') {
        driveCode = '4x4 (AWD)';
        break;
      }
    }
  }
  // Add the drive code to the custom transmisie metafield if found.
  row['Metafield: custom.transmisie [list.single_line_text_field]'] = driveCode;

  // === VAT/TVA classification ===
  // The `Metafield: custom.tva [single_line_text_field]` represents the
  // tax status of the vehicle.  Shopify expects the value to be one of
  // "Deductibile" or "Non Deductibile".  According to current
  // business rules, all exported vehicles should be marked as
  // "Deductibile" by default.  Set the field accordingly.  If
  // future requirements call for conditional logic (e.g. based on
  // VAT presence), adjust this assignment here.
  row['Metafield: custom.tva [single_line_text_field]'] = 'Deductibile';

  // === Template Suffix ===
  // Add a default template suffix for Shopify product import.  This
  // value determines which Liquid template (without the .liquid
  // extension) is used to render the product page.  We default to
  // "produs_servicii" per the user’s requirement.  Change this
  // string if your theme uses a different suffix.
  row['Template Suffix'] = 'produs_servicii';
  return row;
}

/*
 * Map an entire array of raw dataset items to Shopify metafields rows using
 * a provided mapping list.  Returns a new array of objects.
 */
/**
 * Map each raw dataset item into a Shopify-ready row using the
 * provided metafields mapping list.  The mapping process builds a
 * complete set of Shopify columns via buildMetafieldsRow() and then
 * applies mapToNewMetafields() to rename and filter fields according
 * to the NEW_METAFIELD_MAPPINGS table.  Only fields listed in
 * NEW_METAFIELD_MAPPINGS or ALWAYS_KEEP_FIELDS are retained in the
 * final output.
 *
 * @param {Array} items The raw Apify dataset items.
 * @param {Array} mappingList The list of source→dest metafield mappings loaded from the Excel file.
 * @returns {Array} An array of rows ready for Shopify import.
 */
function mapDatasetToMetafields(items, mappingList) {
  return items.map((item) => {
    const row = buildMetafieldsRow(item, mappingList);
    return mapToNewMetafields(row);
  });
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
 * GET /api/normalize-results
 *
 * Normalize a dataset from a completed crawl.  This endpoint fetches
 * raw items from the dataset associated with the given runId, applies
 * the normalizeItem() helper to flatten arrays and objects into simple
 * fields (images, features, attributes), and returns the normalized
 * data in the requested format (json, csv or xlsx).  This allows
 * users to perform step‑by‑step processing: first crawl, then
 * normalize, then optionally map to Shopify.  The run must be
 * finished before calling this endpoint.
 */
app.get('/api/normalize-results', async (req, res) => {
  try {
    const { runId, format = 'json' } = req.query;
    if (!runId) {
      return res.status(400).json({ error: 'runId query parameter is required' });
    }
    const token = process.env.APIFY_TOKEN || process.env.APIFY_API_TOKEN;
    if (!token) {
      return res.status(500).json({ error: 'APIFY_TOKEN or APIFY_API_TOKEN must be set' });
    }
    // Fetch run details to get the default dataset ID
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
    // Normalize each item.
    const normalized = items.map((itm) => normalizeItem(itm));
    const fmt = String(format).toLowerCase();
    if (fmt === 'json') {
      return res.json(normalized);
    }
    if (fmt === 'csv') {
      const csv = toCsv(normalized);
      res.set('Content-Type', 'text/csv');
      res.set('Content-Disposition', `attachment; filename="${runId}-normalized.csv"`);
      return res.send(csv);
    }
    if (fmt === 'xlsx') {
      const workbook = new ExcelJS.Workbook();
      const worksheet = workbook.addWorksheet('Normalized');
      // Determine headers from normalized objects
      const headerSet = new Set();
      normalized.forEach((row) => Object.keys(row).forEach((k) => headerSet.add(k)));
      const headers = Array.from(headerSet);
      worksheet.addRow(headers);
      normalized.forEach((row) => {
        const line = headers.map((h) => row[h] ?? '');
        worksheet.addRow(line);
      });
      const buffer = await workbook.xlsx.writeBuffer();
      res.set('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.set('Content-Disposition', `attachment; filename="${runId}-normalized.xlsx"`);
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
 * POST /api/start-crawl
 *
 * Kick off an Apify actor run asynchronously. Accepts JSON body with
 * "searchUrl" and optional "maxItems". Returns the ID of the run. If
 * APIFY_USE_ACTOR is false, this endpoint is disabled to prevent
 * accidental misuse.
 */
// Start a crawl run by launching the Apify actor with the provided search URL.
// This endpoint initiates a crawl and returns a runId.  It does not
// perform any normalization or Shopify mapping.
app.post('/api/start-crawl', async (req, res) => {
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
    // Build actor input using the new Mobile.de scraper schema.
    // The actor expects searchPageURLs at the top level of the input,
    // along with additional parameters for pagination, reviews and
    // category.  We map the user-provided search URL into the
    // searchPageURLs array and set sensible defaults for other
    // properties.  maxItems controls the maximum number of results
    // to retrieve.
    const max = parseInt(maxItems);
    const input = {
      searchPageURLs: [searchUrl],
      // Increase the default page limit to ensure all results from the
      // provided search URL are captured.  The user can adjust this via
      // environment variables if needed.
      searchPageURLMaxItems: 5000,
      // Disable reviews extraction by default (0 = no reviews).
      reviewLimit: 0,
      // Enable automatic paging through search results.
      automaticPaging: true,
      // Default category to Car to match typical mobile.de searches.
      searchCategory: 'Car',
      // Empty arrays indicate no additional search terms or model filters.
      searchTerms: [],
      models: [],
      // Use the default sort order; can be changed via other inputs
      sort: 'Standard'
    };
    if (!Number.isNaN(max) && max > 0) input.maxItems = max;
    // Start the actor run asynchronously.
    //
    // NOTE: Pass the input object directly as the request body rather than
    // wrapping it under an "input" property. According to Apify's API
    // documentation, the POST payload itself is treated as the Actor's
    // input【673632852931336†L0-L0】. This ensures that only the keys defined in
    // our input object are sent to the Actor and avoids injecting
    // deprecated `start_urls` or other default keys into the run
    // configuration. This also aligns with the working example provided by
    // the user, where the JSON payload defines `searchPageURLs`,
    // `searchPageURLMaxItems`, `reviewLimit`, `automaticPaging`,
    // `searchCategory`, `searchTerms`, `models`, `sort` and `maxItems`
    // directly at the top level.
    const runResp = await fetch(`https://api.apify.com/v2/acts/${actorId}/runs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify(input)
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

// Start a crawl run under the legacy path.  This route simply calls the
// handler defined for /api/start-crawl.  Defining it explicitly avoids
// reliance on internal router internals and ensures that both paths
// are available regardless of load order.
app.post('/api/start-run', async (req, res) => {
  // reuse the /api/start-crawl handler directly
  const handler = app._router.stack.find(
    (layer) => layer.route && layer.route.path === '/api/start-crawl'
  )?.route.stack[0].handle;
  if (handler) {
    return handler(req, res);
  }
  return res.status(404).json({ error: 'Start crawl handler not found' });
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