/**
 * Main application module for the Ingress Inventory Viewer
 * Handles initialization, state management, and coordination between modules
 */

// Application state
let appState = {
  rawData: null,
  processedData: null,
  fileInfo: null,
  userLocation: null,

  // UI state
  keySearchQuery: '',
  keySearchShouldRefocus: false,
  keySearchSelection: null,

  // Sort configuration for keys
  sortConfig: {
    mode: 'alpha', // 'alpha' | 'count' | 'time' | 'distance'
    directions: {
      alpha: 'asc',     // 'asc' (A-Z) | 'desc' (Z-A)
      count: 'asc',     // 'asc' (least→most) | 'desc' (most→least)
      time: 'asc',      // 'asc' (oldest→youngest) | 'desc' (youngest→oldest)
      distance: 'asc'   // 'asc' (closest→farthest) | 'desc' (farthest→closest)
    }
  }
};

// DOM element references (cached for performance)
const elements = {};

/**
 * Initializes the application
 */
function initializeApp() {
  // Cache DOM elements
  cacheElements();

  // Set initial UI state
  UI.showUploadInterface();

  // Set up event listeners
  setupEventListeners();

  // Initialize geolocation for distance calculations
  initializeGeolocation();
}

/**
 * Caches frequently used DOM elements
 */
function cacheElements() {
  elements.fileInput = document.getElementById('fileInput');
  elements.uploadBtn = document.getElementById('uploadBtn');
  elements.dragOverlay = document.getElementById('dragOverlay');
  elements.clearBtn = document.getElementById('clear');
  elements.filterRarity = document.getElementById('filterRarity');
  elements.hideCapsuled = document.getElementById('hideCapsuled');
}

/**
 * Sets up all event listeners for the application
 */
function setupEventListeners() {
  // File upload events
  if (elements.uploadBtn) {
    elements.uploadBtn.addEventListener('click', triggerFileUpload);
  }

  if (elements.fileInput) {
    elements.fileInput.addEventListener('change', handleFileUpload);
  }

  // Drag and drop events
  setupDragAndDrop();

  // Filter change events
  if (elements.filterRarity) {
    elements.filterRarity.addEventListener('change', handleFilterChange);
  }

  if (elements.hideCapsuled) {
    elements.hideCapsuled.addEventListener('change', handleFilterChange);
  }

  // Clear data event
  if (elements.clearBtn) {
    elements.clearBtn.addEventListener('click', clearInventoryData);
  }

  // Custom events for UI components
  document.addEventListener('keySearch', handleKeySearch);
  document.addEventListener('sortChange', handleSortChange);
}

/**
 * Sets up drag and drop functionality
 */
function setupDragAndDrop() {
  let dragCounter = 0;

  document.body.addEventListener('dragenter', (e) => {
    e.preventDefault();
    dragCounter++;

    if (e.dataTransfer.types.includes('Files')) {
      elements.dragOverlay.classList.add('active');
    }
  });

  document.body.addEventListener('dragleave', (e) => {
    e.preventDefault();
    dragCounter--;

    if (dragCounter <= 0) {
      elements.dragOverlay.classList.remove('active');
      dragCounter = 0;
    }
  });

  document.body.addEventListener('dragover', (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  });

  document.body.addEventListener('drop', (e) => {
    e.preventDefault();
    dragCounter = 0;
    elements.dragOverlay.classList.remove('active');

    const files = e.dataTransfer.files;
    if (files.length > 0) {
      const file = files[0];
      if (file.type === 'application/json' || file.name.endsWith('.json')) {
        elements.fileInput.files = files;
        handleFileUpload();
      } else {
        UI.updateSummary('Please drop a JSON file.');
      }
    }
  });
}

/**
 * Triggers file upload dialog
 */
function triggerFileUpload() {
  elements.fileInput.click();
}

/**
 * Handles file upload and processing
 */
async function handleFileUpload() {
  const file = elements.fileInput.files[0];
  if (!file) {
    UI.updateSummary('No file selected.');
    return;
  }

  UI.updateSummary('Reading file...');

  try {
    // Read file content
    const text = await file.text();
    UI.updateSummary('Cleaning and validating JSON...');

    // Clean and parse JSON
    const cleanedJson = UTILS.cleanJsonString(text);
    const parseResult = UTILS.safeJsonParse(cleanedJson);

    if (!parseResult.success) {
      UI.updateSummary(`Invalid JSON file: ${parseResult.error}`);
      return;
    }

    // Validate data structure
    const validationResult = UTILS.validateInventoryData(parseResult.data);
    if (!validationResult.isValid) {
      UI.updateSummary(validationResult.error);
      return;
    }

    // Process the data
    const processedData = DATA.processInventoryData(parseResult.data);

    // Update application state
    appState.rawData = parseResult.data;
    appState.processedData = processedData;

    // Format and store file info
    const fileDate = new Date(file.lastModified);
    const formattedDate = fileDate.toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    });

    appState.fileInfo = `Loaded: ${file.name} - Last modified: ${formattedDate}`;

    // Update UI
    UI.showInventoryInterface();
    UI.updateTotalCount(processedData.totalCount);
    UI.updateSummary(appState.fileInfo);

    // Populate filters and render inventory
    updateFiltersAndRender();

    // Clear the file input
    elements.fileInput.value = '';

  } catch (error) {
    UI.updateSummary(`Failed to read file: ${error.message}`);
  }
}

/**
 * Updates filter options and renders the inventory
 */
function updateFiltersAndRender() {
  if (!appState.processedData) return;

  // Populate filter options
  const { types, rarities } = DATA.populateFilterOptions(appState.processedData.expanded);
  UI.populateRarityFilter(rarities);

  // Render inventory
  renderInventory();
}

/**
 * Renders the current inventory based on filters and state
 */
function renderInventory() {
  if (!appState.processedData) return;

  // Get current filters
  const filters = UI.getCurrentFilters();

  // Filter items
  const filteredItems = DATA.filterItems(appState.processedData.expanded, filters);

  // Group items
  const typeBuckets = DATA.groupItems(filteredItems);

  // Render UI
  UI.renderInventory(typeBuckets, {
    filters,
    sortConfig: appState.sortConfig,
    userLocation: appState.userLocation,
    keySearchQuery: appState.keySearchQuery,
    keySearchShouldRefocus: appState.keySearchShouldRefocus,
    keySearchSelection: appState.keySearchSelection
  });

  // Reset refocus state
  appState.keySearchShouldRefocus = false;

  // Preserve file info in summary
  if (appState.fileInfo) {
    UI.updateSummary(appState.fileInfo);
  }
}

/**
 * Handles filter changes (rarity, hide capsuled)
 */
function handleFilterChange() {
  if (appState.processedData) {
    renderInventory();
  }
}

/**
 * Handles key search input
 * @param {CustomEvent} event - Key search event
 */
function handleKeySearch(event) {
  const { query } = event.detail;

  // Capture selection state for refocusing
  const keySearchInput = document.getElementById('keySearch');
  if (keySearchInput) {
    try {
      appState.keySearchSelection = [keySearchInput.selectionStart, keySearchInput.selectionEnd];
    } catch (error) {
      appState.keySearchSelection = null;
    }
  }

  // Update search state
  appState.keySearchQuery = query.toLowerCase();
  appState.keySearchShouldRefocus = true;

  // Re-render inventory with search filter
  if (appState.processedData) {
    renderInventory();
  }
}

/**
 * Handles sort mode and direction changes for keys
 * @param {CustomEvent} event - Sort change event
 */
function handleSortChange(event) {
  const { mode, direction } = event.detail;

  // Update sort configuration
  appState.sortConfig.mode = mode;
  appState.sortConfig.directions[mode] = direction;

  // Re-render inventory with new sort order
  if (appState.processedData) {
    renderInventory();
  }
}

/**
 * Clears all inventory data and returns to upload interface
 */
function clearInventoryData() {
  // Reset application state
  appState.rawData = null;
  appState.processedData = null;
  appState.fileInfo = null;
  appState.keySearchQuery = '';
  appState.keySearchShouldRefocus = false;
  appState.keySearchSelection = null;

  // Reset sort configuration
  appState.sortConfig = {
    mode: 'alpha',
    directions: {
      alpha: 'asc',
      count: 'asc',
      time: 'asc',
      distance: 'asc'
    }
  };

  // Clear UI
  const itemsEl = document.getElementById('items');
  if (itemsEl) itemsEl.innerHTML = '';

  UI.updateTotalCount(0);
  UI.updateSummary('');

  // Reset filters
  if (elements.filterRarity) elements.filterRarity.value = '';

  // Return to upload interface
  UI.showUploadInterface();
}

/**
 * Initializes geolocation for distance calculations
 */
function initializeGeolocation() {
  try {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          appState.userLocation = {
            lat: position.coords.latitude,
            lon: position.coords.longitude
          };

          // User location acquired for distance calculations

          // Re-render if we have data (for distance calculations)
          if (appState.processedData) {
            renderInventory();
          }
        },
        (error) => {
          // Geolocation unavailable - distance sorting disabled
        },
        CONSTANTS.UI_CONFIG.GEOLOCATION_OPTIONS
      );
    }
  } catch (error) {
    // Geolocation initialization failed - distance sorting disabled
  }
}

/**
 * Gets current application state (for debugging)
 * @returns {Object} Current app state
 */
function getAppState() {
  return { ...appState };
}

/**
 * Exports for development/debugging
 */
if (typeof window !== 'undefined') {
  window.APP = {
    getState: getAppState,
    renderInventory,
    clearInventoryData
  };
}

// Initialize application when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initializeApp);
} else {
  initializeApp();
}