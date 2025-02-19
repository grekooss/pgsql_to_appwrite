import dotenv from "dotenv";
import pg from "pg";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import fetch from "node-fetch";

// Konfiguracja środowiska
const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: join(__dirname, ".env") });

// Konfiguracja PostgreSQL
const pgClient = new pg.Client({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT,
});

// Lista serwerów Nominatim do rotacji
const NOMINATIM_SERVERS = [
  'https://nominatim.openstreetmap.org',
  'https://nominatim.geocoding.ai'
];

let currentServerIndex = 0;
const MAX_RETRIES = 3;

// Funkcja do pobierania następnego serwera
function getNextServer() {
  currentServerIndex = (currentServerIndex + 1) % NOMINATIM_SERVERS.length;
  return NOMINATIM_SERVERS[currentServerIndex];
}

async function getReverseGeocoding(lat, lon, zoom = 18, retryCount = 0) {
  const server = getNextServer();
  const url = `${server}/reverse?format=json&lat=${lat}&lon=${lon}&zoom=${zoom}`;
  
  try {
    console.log(`Zapytanie do Nominatim: ${lat}, ${lon}`);
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'PostgreSQL OSM Data Enrichment Tool/1.0'
      },
      timeout: 5000
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    if (data && data.address) {
      console.log('Otrzymano dane z Nominatim:', JSON.stringify(data.address, null, 2));
    }
    await new Promise(resolve => setTimeout(resolve, 250));
    return data;
  } catch (error) {
    console.error(`Błąd Nominatim: ${error.message}`);
    if (retryCount < MAX_RETRIES) {
      return getReverseGeocoding(lat, lon, zoom, retryCount + 1);
    }
    return null;
  }
}

async function getAddressData(lat, lon) {
  // Spróbuj różnych poziomów przybliżenia
  const zoomLevels = [18, 16, 14, 12];
  
  for (const zoom of zoomLevels) {
    const data = await getReverseGeocoding(lat, lon, zoom);
    if (data && data.address && (data.address.road || data.address.pedestrian)) {
      return data;
    }
  }
  
  return null;
}

async function createRequiredColumns() {
  try {
    console.log('Rozpoczynam tworzenie kolumn...');
    const columns = [
      'addr_street TEXT',
      'addr_city TEXT',
      'addr_country TEXT'
    ];
    
    for (const column of columns) {
      const [name, type] = column.split(' ');
      try {
        console.log(`Próba dodania kolumny ${name}...`);
        await pgClient.query(`
          ALTER TABLE planet_osm_polygon 
          ADD COLUMN IF NOT EXISTS ${name} ${type};
        `);
        console.log(`Dodano kolumnę ${name}`);
      } catch (error) {
        console.error(`Błąd podczas dodawania kolumny ${name}:`, error.message);
        if (!error.message.includes('already exists')) {
          throw error;
        }
      }
    }
    console.log('Zakończono tworzenie kolumn');
  } catch (error) {
    console.error('Błąd podczas tworzenia kolumn:', error);
    throw error;
  }
}

async function connectToDatabase() {
  try {
    console.log('Próba połączenia z bazą danych...');
    await pgClient.connect();
    console.log('Połączono z PostgreSQL');
  } catch (error) {
    console.error('Błąd połączenia z bazą:', error);
    throw error;
  }
}

async function updateAddressData(osmId, street, city, country) {
  try {
    const query = `
      UPDATE planet_osm_polygon 
      SET 
        addr_street = $1,
        addr_city = $2,
        addr_country = $3
      WHERE osm_id = $4
      RETURNING osm_id`;
    
    const result = await pgClient.query(query, [street, city, country, osmId]);
    return result.rowCount > 0;
  } catch (error) {
    return false;
  }
}

// Funkcja do tworzenia paska postępu
function createProgressBar(progress, length = 30) {
  const filledLength = Math.round(length * progress);
  const empty = length - filledLength;
  return '[' + '='.repeat(filledLength) + '-'.repeat(empty) + ']';
}

// Funkcja do formatowania czasu
function formatTime(ms) {
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remainingSeconds}s`;
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m ${remainingSeconds}s`;
}

// Funkcja do wyświetlania postępu
function displayProgress(stats, batchProgress, batchSize, startTime) {
  if (stats.recentActions.length > 0) {
    stats.recentActions.forEach(action => console.log(action));
  }
}

async function getGeoPointsWithAddresses() {
  try {
    await connectToDatabase();
    console.log("Sprawdzono i dodano brakujące kolumny");
    await createRequiredColumns();

    const stats = {
      totalProcessed: 0,
      successfulUpdates: 0,
      failedUpdates: 0,
      serverStats: {},
      recentActions: [],
      startTime: Date.now()
    };

    // Inicjalizacja statystyk serwerów
    NOMINATIM_SERVERS.forEach(server => {
      stats.serverStats[server] = { total: 0, success: 0 };
    });

    const batchSize = 100;
    let hasMoreRecords = true;
    const concurrentRequests = 4;

    // Funkcja do dodawania akcji
    const addAction = (action) => {
      stats.recentActions.push(`[${new Date().toLocaleTimeString()}] ${action}`);
      if (stats.recentActions.length > 10) stats.recentActions.shift();
    };

    while (hasMoreRecords) {
      const query = `
        SELECT 
          osm_id,
          ROUND(ST_Y(ST_Transform(ST_Centroid(way), 4326))::numeric, 6) as lat,
          ROUND(ST_X(ST_Transform(ST_Centroid(way), 4326))::numeric, 6) as lon
        FROM planet_osm_polygon 
        WHERE building IS NOT NULL 
          AND addr_street IS NULL 
        LIMIT $1
      `;
      
      const result = await pgClient.query(query, [batchSize]);
      
      if (result.rows.length === 0) {
        hasMoreRecords = false;
        break;
      }

      addAction(`Rozpoczęto przetwarzanie partii ${result.rows.length} rekordów`);

      for (let i = 0; i < result.rows.length; i += concurrentRequests) {
        const batch = result.rows.slice(i, i + concurrentRequests);
        const promises = batch.map(row => processRecord(row, stats, addAction));
        
        await Promise.all(promises);
        
        // Aktualizuj wyświetlanie
        displayProgress(stats, i + batch.length, result.rows.length, stats.startTime);
      }

      if (result.rows.length < batchSize) {
        hasMoreRecords = false;
      }
    }

    // Podsumowanie końcowe
    console.log('\n' + '='.repeat(50));
    console.log('PODSUMOWANIE KOŃCOWE');
    console.log('='.repeat(50));
    console.log(`\nCzas wykonania: ${formatTime(Date.now() - stats.startTime)}`);
    console.log(`Łącznie przetworzono: ${stats.totalProcessed} rekordów`);
    console.log(`Udane aktualizacje: ${stats.successfulUpdates}`);
    console.log(`Nieudane aktualizacje: ${stats.failedUpdates}`);
    
    // Statystyki serwerów
    console.log('\nWydajność serwerów Nominatim:');
    for (const server of NOMINATIM_SERVERS) {
      const serverStats = stats.serverStats[server];
      const successRate = ((serverStats.success / serverStats.total) * 100).toFixed(1);
      console.log(`${server}: ${successRate}% sukcesu (${serverStats.success}/${serverStats.total})`);
    }

    // Pokaż przykładowe zaktualizowane rekordy
    if (stats.successfulUpdates > 0) {
      console.log("\nPrzykładowe ostatnio zaktualizowane rekordy:");
      const checkQuery = `
        SELECT osm_id, addr_street, addr_city, addr_country 
        FROM planet_osm_polygon 
        WHERE addr_street IS NOT NULL 
        ORDER BY osm_id DESC
        LIMIT 5;
      `;
      const checkResult = await pgClient.query(checkQuery);
      console.table(checkResult.rows);
    }

  } catch (error) {
    console.error("Wystąpił błąd:", error);
  } finally {
    await pgClient.end();
    console.log("\nZakończono połączenie z bazą danych");
  }
}

// Aktualizacja funkcji processRecord aby generowała mniej szumu w logach
async function processRecord(row, stats, addAction) {
  try {
    const addressData = await getAddressData(row.lat, row.lon);
    const server = NOMINATIM_SERVERS[currentServerIndex];
    stats.serverStats[server].total++;
    
    if (addressData && addressData.address) {
      const street = addressData.address.road || 
                    addressData.address.pedestrian || 
                    addressData.address.path ||
                    addressData.address.footway ||
                    addressData.address.street ||
                    addressData.address.village ||
                    'bez nazwy';
                    
      const city = addressData.address.city || 
                  addressData.address.town || 
                  addressData.address.village || 
                  addressData.address.municipality ||
                  addressData.address.county;
                  
      const country = addressData.address.country;

      if (city && country) {
        const updated = await updateAddressData(row.osm_id, street, city, country);
        if (updated) {
          stats.successfulUpdates++;
          stats.serverStats[server].success++;
          console.log(`Dodano: ${street}, ${city}, ${country}`);
        } else {
          stats.failedUpdates++;
        }
      } else {
        stats.failedUpdates++;
      }
    } else {
      stats.failedUpdates++;
    }
    stats.totalProcessed++;
  } catch (error) {
    stats.failedUpdates++;
    stats.totalProcessed++;
  }
}

// Uruchom program
getGeoPointsWithAddresses();
