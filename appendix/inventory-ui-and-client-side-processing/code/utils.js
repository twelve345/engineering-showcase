/**
 * Utility functions for the Ingress Inventory Viewer
 * Contains helper functions for data transformation, calculations, and formatting
 */

/**
 * Sanitizes JSON string by removing invalid control characters
 * @param {string} jsonString - Raw JSON string to clean
 * @returns {string} Cleaned JSON string safe for parsing
 */
function cleanJsonString(jsonString) {
  const validControlChars = CONSTANTS.JSON_VALIDATION.VALID_CONTROL_CHARS;
  const minPrintable = CONSTANTS.JSON_VALIDATION.MIN_PRINTABLE_CHAR;

  let cleaned = '';
  let removedCount = 0;

  for (let i = 0; i < jsonString.length; i++) {
    const charCode = jsonString.charCodeAt(i);

    // Keep printable characters and valid whitespace
    if (charCode >= minPrintable || validControlChars.has(charCode)) {
      cleaned += jsonString[i];
    } else {
      removedCount++;
    }
  }

  if (removedCount > 0) {
    console.log(`Cleaned JSON: removed ${removedCount} invalid control characters`);
  }
  return cleaned;
}

/**
 * Formats timestamp to localized string
 * @param {number|string} ms - Timestamp in milliseconds
 * @returns {string} Formatted date string or empty string if invalid
 */
function formatLocalTs(ms) {
  const n = Number(ms);
  if (!n || !isFinite(n)) return '';

  const d = new Date(n);
  if (isNaN(d.getTime())) return '';

  return d.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZoneName: 'short'
  });
}

/**
 * Decodes Ingress portal location from hex format
 * @param {string} locationString - Hex encoded location "lat,lon"
 * @returns {Object|null} {lat: number, lon: number} or null if invalid
 */
function decodePortalLocation(locationString) {
  if (!locationString || typeof locationString !== 'string' || !locationString.includes(',')) {
    return null;
  }

  const [latHex, lonHex] = locationString.split(',');

  // Convert hex to signed 32-bit integer
  const toSigned = (n) => (n & 0x80000000) ? n - 0x100000000 : n;

  const latE6 = toSigned(parseInt(latHex, 16));
  const lonE6 = toSigned(parseInt(lonHex, 16));

  if (!isFinite(latE6) || !isFinite(lonE6)) return null;

  return {
    lat: latE6 / 1e6,
    lon: lonE6 / 1e6
  };
}

/**
 * Calculates distance between two coordinates using Haversine formula
 * @param {number} lat1 - Latitude of first point
 * @param {number} lon1 - Longitude of first point
 * @param {number} lat2 - Latitude of second point
 * @param {number} lon2 - Longitude of second point
 * @returns {number} Distance in kilometers
 */
function haversineKm(lat1, lon1, lat2, lon2) {
  const R = CONSTANTS.UI_CONFIG.EARTH_RADIUS_KM;
  const toRad = d => d * Math.PI / 180;

  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const a = Math.sin(dLat / 2) ** 2 +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
            Math.sin(dLon / 2) ** 2;

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}

/**
 * Converts kilometers to miles
 * @param {number} km - Distance in kilometers
 * @returns {number} Distance in miles
 */
function kmToMiles(km) {
  return km * CONSTANTS.UI_CONFIG.KM_TO_MILES_FACTOR;
}

/**
 * Abbreviates rarity strings for display
 * @param {string} rarity - Full rarity string
 * @returns {string} Abbreviated rarity or empty string
 */
function abbreviateRarity(rarity) {
  return CONSTANTS.RARITY.ABBREVIATIONS[rarity] || rarity || '';
}

/**
 * Maps resource type to display category
 * @param {string} resourceType - Raw resource type from game data
 * @returns {string} Display category name
 */
function getDisplayType(resourceType) {
  const { ITEM_TYPES } = CONSTANTS;

  if (resourceType === 'EMITTER_A') return ITEM_TYPES.DISPLAY_TYPES.RESONATORS;
  if (resourceType === 'PORTAL_LINK_KEY') return ITEM_TYPES.DISPLAY_TYPES.KEYS;
  if (resourceType === 'MEDIA') return ITEM_TYPES.DISPLAY_TYPES.MEDIA;
  if (resourceType === 'PORTAL_POWERUP' || resourceType === 'PLAYER_POWERUP') {
    return ITEM_TYPES.DISPLAY_TYPES.POWERUPS;
  }

  if (ITEM_TYPES.CAPSULE_TYPES.includes(resourceType)) {
    return ITEM_TYPES.DISPLAY_TYPES.CAPSULES;
  }
  if (ITEM_TYPES.MOD_TYPES.includes(resourceType)) {
    return ITEM_TYPES.DISPLAY_TYPES.MODS;
  }
  if (ITEM_TYPES.WEAPON_TYPES.includes(resourceType)) {
    return ITEM_TYPES.DISPLAY_TYPES.WEAPONS;
  }
  if (ITEM_TYPES.CUBE_TYPES.includes(resourceType)) {
    return ITEM_TYPES.DISPLAY_TYPES.CUBES;
  }

  return resourceType;
}

/**
 * Determines if an item is stored in a capsule
 * @param {Object} meta - Item metadata
 * @returns {boolean} True if item is capsuled
 */
function isCapsuled(meta) {
  const containerType = meta?._storedIn?.containerType;
  if (!containerType) return false;

  return CONSTANTS.ITEM_TYPES.CAPSULE_TYPES.includes(containerType);
}

/**
 * Gets diamond configuration for mod visualization
 * @param {Object} meta - Item metadata
 * @returns {Object|null} {count: number, color: string} or null
 */
function getModDiamondConfig(meta) {
  const mr = meta?.modResource;
  if (!mr) return null;

  const { MOD_VISUALS } = CONSTANTS;
  const name = (mr.displayName || '').toLowerCase();

  // Special case for Aegis/AXA shields
  const isAegisAxa = mr.resourceType === 'EXTRA_SHIELD' &&
                    (name.includes('aegis') || name.includes('axa'));

  if (isAegisAxa) return MOD_VISUALS.DIAMOND_CONFIG.AEGIS_AXA;

  // Standard rarity-based configuration
  const rarity = mr.rarity;
  return MOD_VISUALS.DIAMOND_CONFIG[rarity] || null;
}

/**
 * Generates display title for an item
 * @param {string} id - Item ID
 * @param {Object} meta - Item metadata
 * @returns {string} Display title
 */
function displayTitle(id, meta) {
  // Mod resources - show rarity abbreviation
  if (meta.modResource) {
    return abbreviateRarity(meta.modResource.rarity || '');
  }

  // Portal keys - show portal title
  if (meta.portalCoupler?.portalTitle) {
    return meta.portalCoupler.portalTitle;
  }

  // Story items (media) - show description
  if (meta.storyItem?.shortDescription) {
    return meta.storyItem.shortDescription;
  }

  // Level-based items
  if (meta.resourceWithLevels?.resourceType) {
    const type = meta.resourceWithLevels.resourceType;
    const level = meta.resourceWithLevels.level || '';

    // Simple level display for common items
    if (['EMITTER_A', 'POWER_CUBE', 'EMP_BURSTER', 'ULTRA_STRIKE'].includes(type)) {
      return `L${level}`;
    }

    return `${type} L${level}`;
  }

  // Resource-based items
  if (meta.resource?.resourceType) {
    const type = meta.resource.resourceType;

    // Special cases with custom names
    if (type === 'FLIP_CARD' && meta.flipCard?.flipCardType) {
      return meta.flipCard.flipCardType;
    }

    if (type === 'BOOSTED_POWER_CUBE') {
      return 'Hyper';
    }

    // Powerups - show designation/enum for grouping
    if (type === 'PORTAL_POWERUP' && meta.timedPowerupResource?.designation) {
      return meta.timedPowerupResource.designation;
    }

    if (type === 'PLAYER_POWERUP' && meta.playerPowerupResource?.playerPowerupEnum) {
      return meta.playerPowerupResource.playerPowerupEnum;
    }

    return type;
  }

  // Fallback to item ID
  return id;
}

/**
 * Creates a debounced version of a function
 * @param {Function} func - Function to debounce
 * @param {number} wait - Wait time in milliseconds
 * @returns {Function} Debounced function
 */
function debounce(func, wait) {
  let timeout;

  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };

    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

/**
 * Safely parses JSON with error handling
 * @param {string} jsonString - JSON string to parse
 * @returns {Object} {success: boolean, data?: any, error?: string}
 */
function safeJsonParse(jsonString) {
  try {
    const data = JSON.parse(jsonString);
    return { success: true, data };
  } catch (error) {
    return {
      success: false,
      error: error.message
    };
  }
}

/**
 * Validates inventory data structure
 * @param {any} data - Parsed JSON data
 * @returns {Object} {isValid: boolean, error?: string}
 */
function validateInventoryData(data) {
  if (!data) {
    return { isValid: false, error: 'No data provided' };
  }

  if (!data.result && !Array.isArray(data)) {
    return {
      isValid: false,
      error: 'Invalid inventory file format. Expected JSON with "result" array.'
    };
  }

  return { isValid: true };
}

// Export all utility functions
if (typeof module !== 'undefined' && module.exports) {
  // Node.js environment
  module.exports = {
    cleanJsonString,
    formatLocalTs,
    decodePortalLocation,
    haversineKm,
    kmToMiles,
    abbreviateRarity,
    getDisplayType,
    isCapsuled,
    getModDiamondConfig,
    displayTitle,
    debounce,
    safeJsonParse,
    validateInventoryData
  };
} else {
  // Browser environment - attach to window
  window.UTILS = {
    cleanJsonString,
    formatLocalTs,
    decodePortalLocation,
    haversineKm,
    kmToMiles,
    abbreviateRarity,
    getDisplayType,
    isCapsuled,
    getModDiamondConfig,
    displayTitle,
    debounce,
    safeJsonParse,
    validateInventoryData
  };
}