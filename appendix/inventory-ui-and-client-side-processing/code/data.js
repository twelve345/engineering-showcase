/**
 * Data processing module for the Ingress Inventory Viewer
 * Handles JSON parsing, item extraction, grouping, filtering, and sorting
 */

/**
 * Extracts nested items from containers (capsules) and flattens inventory structure
 * @param {Array} itemArray - Array of inventory items from IITC export
 * @returns {Array} Expanded array with container items extracted
 */
function extractNestedItems(itemArray) {
  const itemMap = new Map();
  const expandedItems = [];

  // First pass: Create lookup map of all items by ID
  for (const item of itemArray) {
    const id = item[0];
    itemMap.set(id, item);
  }

  // Second pass: Process each item and extract nested contents
  for (const item of itemArray) {
    const [id, timestamp, meta] = item;
    expandedItems.push(item);

    // Check if this item is a container with nested items
    if (meta.container?.stackableItems) {

      for (const stackableItem of meta.container.stackableItems) {
        // Handle items with both itemGuids and exampleGameEntity (like keys in KEY_CAPSULE)
        if (stackableItem.itemGuids && stackableItem.exampleGameEntity) {
          const template = stackableItem.exampleGameEntity;
          const [templateId, templateTs, templateMeta] = template;

          // Create an item for each GUID using the template
          for (const itemGuid of stackableItem.itemGuids) {
            const enrichedMeta = {
              ...templateMeta,
              _storedIn: {
                containerId: id,
                containerType: meta.resource?.resourceType || 'CONTAINER'
              }
            };

            expandedItems.push([itemGuid, templateTs, enrichedMeta]);
          }
        }
        // Handle items with just exampleGameEntity (single items)
        else if (stackableItem.exampleGameEntity) {
          const embeddedEntity = stackableItem.exampleGameEntity;
          const [embeddedId, embeddedTs, embeddedMeta] = embeddedEntity;
          const enrichedMeta = {
            ...embeddedMeta,
            _storedIn: {
              containerId: id,
              containerType: meta.resource?.resourceType || 'CONTAINER'
            }
          };

          expandedItems.push([embeddedId, embeddedTs, enrichedMeta]);
        }
        // Handle items by GUID only (items that exist in main array)
        else if (stackableItem.itemGuids) {
          for (const nestedId of stackableItem.itemGuids) {
            const nestedItem = itemMap.get(nestedId);
            if (nestedItem) {
              const [nestedItemId, nestedTs, nestedMeta] = nestedItem;
              const enrichedMeta = {
                ...nestedMeta,
                _storedIn: {
                  containerId: id,
                  containerType: meta.resource?.resourceType || 'CONTAINER'
                }
              };

              expandedItems.push([nestedItemId, nestedTs, enrichedMeta]);
            }
          }
        }
      }
    }
  }

  return expandedItems;
}

/**
 * Processes raw inventory data and returns processed structure
 * @param {Object} rawData - Raw JSON data from file
 * @returns {Object} Processed inventory data
 */
function processInventoryData(rawData) {
  const itemArray = Array.isArray(rawData.result) ? rawData.result : rawData;

  // Extract nested items from containers
  const expandedData = extractNestedItems(itemArray);

  // Calculate total excluding keys in key lockers for title display
  let totalCount = expandedData.length;
  try {
    const keyLockerKeyCount = expandedData.reduce((acc, item) => {
      const meta = item[2] || {};
      const inKeyLocker = meta._storedIn?.containerType === 'KEY_CAPSULE';
      const isKey = !!(meta.portalCoupler &&
                      (meta.portalCoupler.resourceType === 'PORTAL_LINK_KEY' ||
                       meta.portalCoupler.portalGuid));
      return acc + (inKeyLocker && isKey ? 1 : 0);
    }, 0);
    totalCount = expandedData.length - keyLockerKeyCount;
  } catch (error) {
    // Fallback to simple count if calculation fails
  }

  return {
    raw: rawData,
    expanded: expandedData,
    totalCount
  };
}

/**
 * Filters inventory items based on current filter settings
 * @param {Array} items - Expanded inventory items
 * @param {Object} filters - Filter configuration
 * @returns {Array} Filtered items
 */
function filterItems(items, filters) {
  return items.filter(item => {
    const [id, timestamp, meta] = item;

    // Always hide Drone items
    const resource = meta.resource || meta.resourceWithLevels || meta.modResource || {};
    if (resource.resourceType === 'DRONE') return false;

    // Rarity filter
    if (filters.rarity) {
      const rarity = resource.resourceRarity || meta.modResource?.rarity;
      if (rarity !== filters.rarity) return false;
    }

    return true;
  });
}

/**
 * Groups items by display type and title for rendering
 * @param {Array} filteredItems - Pre-filtered inventory items
 * @returns {Map} Map of displayType -> Map<groupKey, {items, gmeta}>
 */
function groupItems(filteredItems) {
  const typeBuckets = new Map();

  for (const item of filteredItems) {
    const [id, timestamp, meta] = item;

    const rawType = meta.resourceWithLevels?.resourceType ||
                   meta.resource?.resourceType ||
                   meta.modResource?.resourceType;

    const displayType = UTILS.getDisplayType(rawType);
    const title = UTILS.displayTitle(id, meta);

    // Create unique group key for items that need type differentiation
    const needsTypeInKey = ['Weapons', 'Cubes', 'Mods'].includes(displayType);
    const groupKey = needsTypeInKey ? `${title}|${rawType}` : title;

    // Initialize type bucket if needed
    if (!typeBuckets.has(displayType)) {
      typeBuckets.set(displayType, new Map());
    }

    const grouped = typeBuckets.get(displayType);

    // Initialize group if needed
    if (!grouped.has(groupKey)) {
      const gmeta = { displayType, title, level: 0 };

      // Add type-specific metadata for sorting
      if (displayType === 'Weapons') {
        gmeta.weaponType = rawType || '';
        gmeta.level = meta.resourceWithLevels?.level || 0;
      } else if (displayType === 'Cubes') {
        gmeta.cubeType = rawType || '';
        gmeta.level = meta.resourceWithLevels?.level || 0;
      } else if (displayType === 'Resonators') {
        gmeta.level = meta.resourceWithLevels?.level || 0;
      } else if (displayType === 'Mods') {
        gmeta.modType = rawType || '';
        gmeta.rarity = meta.modResource?.rarity || '';
      }

      grouped.set(groupKey, { items: [], gmeta });
    }

    // Add item to group
    grouped.get(groupKey).items.push({
      id,
      ts: timestamp,
      meta,
      item
    });
  }

  return typeBuckets;
}

/**
 * Sorts groups within a type section based on type-specific rules
 * @param {Array} entries - Array of [groupKey, groupValue] pairs
 * @param {string} displayType - Type of items being sorted
 * @param {Object} sortConfig - Sorting configuration for keys
 * @param {Object} userLocation - User's location for distance sorting
 * @returns {Array} Sorted entries
 */
function sortTypeGroups(entries, displayType, sortConfig = {}, userLocation = null) {
  const { SORT_CONFIG } = CONSTANTS;

  // Keys have special sorting modes
  if (displayType === 'Keys') {
    return sortKeys(entries, sortConfig, userLocation);
  }

  // Weapons: specific type order, then level descending
  if (displayType === 'Weapons') {
    return entries.sort((a, b) => {
      const gmA = a[1].gmeta;
      const gmB = b[1].gmeta;

      const orderA = SORT_CONFIG.WEAPON_ORDER[gmA.weaponType] ?? 9;
      const orderB = SORT_CONFIG.WEAPON_ORDER[gmB.weaponType] ?? 9;

      if (orderA !== orderB) return orderA - orderB;

      const levelA = gmA.level || 0;
      const levelB = gmB.level || 0;
      if (levelA !== levelB) return levelB - levelA;

      const titleA = a[0].split('|')[0];
      const titleB = b[0].split('|')[0];
      return titleA.localeCompare(titleB);
    });
  }

  // Resonators: sort by level (L8 -> L1)
  if (displayType === 'Resonators') {
    return entries.sort((a, b) => {
      const levelA = a[1].gmeta.level || 0;
      const levelB = b[1].gmeta.level || 0;
      return levelB - levelA;
    });
  }

  // Cubes: sort by type then by level (highest first)
  if (displayType === 'Cubes') {
    return entries.sort((a, b) => {
      const typeA = a[1].gmeta.cubeType || '';
      const typeB = b[1].gmeta.cubeType || '';
      if (typeA !== typeB) return typeA.localeCompare(typeB);

      const levelA = a[1].gmeta.level || 0;
      const levelB = b[1].gmeta.level || 0;
      return levelB - levelA;
    });
  }

  // Mods: specific type order, then rarity (VR > R > C)
  if (displayType === 'Mods') {
    return entries.sort((a, b) => {
      const typeA = a[1].gmeta.modType || '';
      const typeB = b[1].gmeta.modType || '';

      const orderA = SORT_CONFIG.MOD_ORDER[typeA] ?? 9;
      const orderB = SORT_CONFIG.MOD_ORDER[typeB] ?? 9;
      if (orderA !== orderB) return orderA - orderB;

      const rarityA = a[1].gmeta.rarity || '';
      const rarityB = b[1].gmeta.rarity || '';
      const rarityOrderA = CONSTANTS.RARITY.SORT_ORDER[rarityB] || 0;
      const rarityOrderB = CONSTANTS.RARITY.SORT_ORDER[rarityA] || 0;

      if (rarityOrderA !== rarityOrderB) return rarityOrderA - rarityOrderB;

      if (typeA !== typeB) return typeA.localeCompare(typeB);

      const titleA = a[0].split('|')[0];
      const titleB = b[0].split('|')[0];
      return titleA.localeCompare(titleB);
    });
  }

  // Powerups: sort by count (most -> least), then by title
  if (displayType === 'Powerups') {
    return entries.sort((a, b) => {
      const countA = a[1].items.length;
      const countB = b[1].items.length;
      if (countA !== countB) return countB - countA;

      const titleA = a[0].split('|')[0];
      const titleB = b[0].split('|')[0];
      return titleA.localeCompare(titleB);
    });
  }

  // Default: alphabetical by title
  return entries.sort((a, b) => {
    const titleA = a[0].split('|')[0];
    const titleB = b[0].split('|')[0];
    return titleA.localeCompare(titleB);
  });
}

/**
 * Sorts key entries based on selected sort mode
 * @param {Array} entries - Key entries to sort
 * @param {Object} sortConfig - Sort configuration {mode, directions}
 * @param {Object} userLocation - User's location for distance sorting
 * @returns {Array} Sorted entries
 */
function sortKeys(entries, sortConfig, userLocation) {
  const { mode, directions } = sortConfig;

  if (mode === 'count') {
    return entries.sort((a, b) => {
      const countA = a[1].items.length;
      const countB = b[1].items.length;
      return directions.count === 'desc' ? (countB - countA) : (countA - countB);
    });
  }

  if (mode === 'time') {
    return entries.sort((a, b) => {
      const timeA = a[1].items.reduce((max, item) => Math.max(max, Number(item.ts) || 0), 0);
      const timeB = b[1].items.reduce((max, item) => Math.max(max, Number(item.ts) || 0), 0);
      return directions.time === 'desc' ? (timeB - timeA) : (timeA - timeB);
    });
  }

  if (mode === 'distance' && userLocation) {
    const getDistance = (entry) => {
      const firstItem = entry[1].items[0];
      const locationString = firstItem?.meta?.portalCoupler?.portalLocation;
      const location = locationString ? UTILS.decodePortalLocation(locationString) : null;

      if (!location) return Number.POSITIVE_INFINITY;

      const km = UTILS.haversineKm(
        userLocation.lat,
        userLocation.lon,
        location.lat,
        location.lon
      );

      return isFinite(km) ? km : Number.POSITIVE_INFINITY;
    };

    return entries.sort((a, b) => {
      const distA = getDistance(a);
      const distB = getDistance(b);
      return directions.distance === 'desc' ? (distB - distA) : (distA - distB);
    });
  }

  // Default: alphabetical
  return entries.sort((a, b) => {
    const titleA = a[0].split('|')[0];
    const titleB = b[0].split('|')[0];
    return directions.alpha === 'desc' ?
           titleB.localeCompare(titleA) :
           titleA.localeCompare(titleB);
  });
}

/**
 * Populates filter dropdown options based on available items
 * @param {Array} expandedData - All inventory items
 * @returns {Object} Filter options {types: Set, rarities: Set}
 */
function populateFilterOptions(expandedData) {
  const types = new Set();
  const rarities = new Set();

  for (const item of expandedData) {
    const meta = item[2] || {};
    const resource = meta.resource || meta.resourceWithLevels || meta.modResource;

    if (resource?.resourceType) {
      // Exclude Drone from type sections
      if (resource.resourceType === 'DRONE') continue;
      types.add(UTILS.getDisplayType(resource.resourceType));
    }

    if (resource?.resourceRarity) {
      rarities.add(resource.resourceRarity);
    }

    if (meta.modResource?.rarity) {
      rarities.add(meta.modResource.rarity);
    }
  }

  return { types, rarities };
}

// Export all data processing functions
if (typeof module !== 'undefined' && module.exports) {
  // Node.js environment
  module.exports = {
    extractNestedItems,
    processInventoryData,
    filterItems,
    groupItems,
    sortTypeGroups,
    sortKeys,
    populateFilterOptions
  };
} else {
  // Browser environment - attach to window
  window.DATA = {
    extractNestedItems,
    processInventoryData,
    filterItems,
    groupItems,
    sortTypeGroups,
    sortKeys,
    populateFilterOptions
  };
}