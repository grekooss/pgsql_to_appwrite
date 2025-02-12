import dotenv from "dotenv";
import { Client, Databases, ID } from "node-appwrite";
import { dirname, join } from "path";
import pg from "pg";
import { fileURLToPath } from "url";

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

// Konfiguracja Appwrite
const appwrite = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const databases = new Databases(appwrite);

// Mapowanie typów PostgreSQL na typy Appwrite
const pgToAppwriteType = {
  integer: "integer",
  bigint: "integer",
  "character varying": "string",
  text: "string",
  boolean: "boolean",
  "timestamp with time zone": "datetime",
  "timestamp without time zone": "datetime",
  date: "datetime",
  "double precision": "double",
  real: "double",
  numeric: "double",
  json: "string",
  jsonb: "string",
};

// Funkcja pomocnicza do normalizacji nazw atrybutów
function normalizeAttributeName(name) {
  return name.replace(/[:"]/g, "_").toLowerCase();
}

async function getTableStructure(tableName) {
  const query = `
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position;
    `;
  const result = await pgClient.query(query, [tableName]);
  return result.rows;
}

async function createAppwriteCollection(tableName, columns) {
  try {
    // Sprawdź czy kolekcja już istnieje
    try {
      await databases.getCollection(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID
      );
      console.log("Kolekcja już istnieje, usuwam...");
      await databases.deleteCollection(
        process.env.APPWRITE_DATABASE_ID,
        process.env.APPWRITE_COLLECTION_ID
      );
    } catch (error) {
      // Kolekcja nie istnieje, to dobrze
    }

    // Tworzenie kolekcji
    await databases.createCollection(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID,
      tableName
    );

    console.log("Utworzono nową kolekcję");

    // Tworzenie atrybutów dla każdej kolumny
    const attributeOrder = [
      "osm_id",
      "name",
      "operator",
      "brand",
      "addr:housename",
      "addr:housenumber",
      "amenity",
      "barrier",
      "building",
      "landuse",
      "leisure",
      "natural",
      "sport",
      "surface",
      "way_area",
      "center_point",
      "way",
    ];

    for (const attrName of attributeOrder) {
      const colName = normalizeAttributeName(attrName);

      try {
        if (attrName === "osm_id") {
          await databases.createStringAttribute(
            process.env.APPWRITE_DATABASE_ID,
            process.env.APPWRITE_COLLECTION_ID,
            colName,
            255,
            true
          );
        } else if (attrName === "way_area") {
          await databases.createFloatAttribute(
            process.env.APPWRITE_DATABASE_ID,
            process.env.APPWRITE_COLLECTION_ID,
            colName,
            false
          );
        } else if (attrName === "way") {
          await databases.createStringAttribute(
            process.env.APPWRITE_DATABASE_ID,
            process.env.APPWRITE_COLLECTION_ID,
            colName,
            100000,
            false
          );
        } else {
          await databases.createStringAttribute(
            process.env.APPWRITE_DATABASE_ID,
            process.env.APPWRITE_COLLECTION_ID,
            colName,
            255,
            false
          );
        }
        console.log(`Dodano atrybut: ${colName}`);
      } catch (error) {
        console.error(`Błąd podczas tworzenia atrybutu ${colName}:`, error);
      }
    }

    // Poczekaj chwilę, aby Appwrite zaktualizował indeksy
    await new Promise((resolve) => setTimeout(resolve, 2000));
  } catch (error) {
    console.error("Błąd podczas tworzenia kolekcji:", error);
    throw error;
  }
}

async function listTables() {
  const query = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
    `;
  const result = await pgClient.query(query);
  return result.rows;
}

async function migrateData() {
  try {
    // Połączenie z PostgreSQL
    await pgClient.connect();
    console.log("Połączono z PostgreSQL");

    // Nazwa tabeli do migracji
    const tableName = "planet_osm_polygon";

    let oversizedWayCount = 0;
    const processedOsmIds = new Set();
    const duplicateOsmIds = new Set();

    // Lista dozwolonych typów budynków
    const allowedBuildingTypes = [
      "apartments",
      "house",
      "hotel",
      "villa",
      "castle",
      "houseboat",
      "farm",
      "cabin",
      "camping",
      "resort",
      "land",
      "caravan",
      "historic",
    ];

    // Wybrane kolumny do migracji (z poprawnymi nazwami SQL)
    const selectedColumns = [
      "osm_id",
      "name",
      "operator",
      "brand",
      "addr:housename",
      "addr:housenumber",
      "amenity",
      "barrier",
      "building",
      "landuse",
      "leisure",
      "natural",
      "sport",
      "surface",
      "way_area",
      "center_point",
      "way",
    ].map((col) => `"${col}"`);

    // Pobierz dane z PostgreSQL
    const query = `
      SELECT 
          ${selectedColumns
            .filter((col) => col !== '"center_point"' && col !== '"way"')
            .join(", ")},
          ARRAY[ST_Y(ST_Transform(ST_Centroid("way"), 4326)), ST_X(ST_Transform(ST_Centroid("way"), 4326))] as "center_point",
          ST_AsGeoJSON(ST_Transform(ST_RemoveRepeatedPoints(ST_Simplify("way", 0.1)), 4326)) as "way"
      FROM ${tableName}
      WHERE "building" = ANY($1)
    `;
    const result = await pgClient.query(query, [allowedBuildingTypes]);
    console.log(`\nZnaleziono ${result.rows.length} rekordów do migracji`);

    if (result.rows.length === 0) {
      console.log("Nie znaleziono żadnych budynków z wybranych kategorii!");
      return;
    }

    // Utwórz kolekcję w Appwrite
    await createAppwriteCollection(tableName, selectedColumns);
    console.log("Utworzono/zaktualizowano kolekcję w Appwrite");

    let buildingTypeStats = {};
    allowedBuildingTypes.forEach((type) => (buildingTypeStats[type] = 0));

    // Migracja każdego rekordu do Appwrite
    for (const row of result.rows) {
      try {
        const osmId = row.osm_id.toString();

        // Sprawdzanie duplikatów
        if (processedOsmIds.has(osmId)) {
          duplicateOsmIds.add(osmId);
          console.log(`Znaleziono duplikat osm_id: ${osmId}`);
          continue;
        }
        processedOsmIds.add(osmId);

        // Zliczanie typów budynków
        const buildingType = row.building;
        if (buildingType && buildingTypeStats.hasOwnProperty(buildingType)) {
          buildingTypeStats[buildingType]++;
        }

        // Konwertuj liczby na stringi, ponieważ Appwrite może mieć problemy z dużymi liczbami
        const wayData = row.way ? JSON.parse(row.way) : null;
        const wayPoints = wayData
          ? wayData.coordinates[0].map((point) => [point[1], point[0]])
          : null;
        const wayJson = wayPoints ? JSON.stringify(wayPoints) : null;

        const isWayOversized = wayJson && wayJson.length > 100000;
        if (isWayOversized) {
          oversizedWayCount++;
        }

        // Przygotuj znormalizowane dane
        const documentData = {};
        for (const [key, value] of Object.entries(row)) {
          const normalizedKey = normalizeAttributeName(key);
          if (normalizedKey === "osm_id") {
            documentData[normalizedKey] = value.toString();
          } else if (normalizedKey === "way_area") {
            documentData[normalizedKey] = value ? parseFloat(value) : null;
          } else if (normalizedKey === "center_point") {
            documentData[normalizedKey] = value ? JSON.stringify(value) : null;
          } else if (normalizedKey === "way") {
            documentData[normalizedKey] = isWayOversized ? null : wayJson;
          } else {
            documentData[normalizedKey] = value === null ? "" : String(value);
          }
        }

        await databases.createDocument(
          process.env.APPWRITE_DATABASE_ID,
          process.env.APPWRITE_COLLECTION_ID,
          ID.unique(),
          documentData
        );
        console.log(
          `Zmigrowano rekord: ${documentData.osm_id} (typ: ${buildingType})`
        );
      } catch (error) {
        console.error("Błąd podczas migracji rekordu:", error);
      }
    }

    console.log("\nRaport z migracji:");
    console.log(
      `Całkowita liczba przetworzonych rekordów: ${result.rows.length}`
    );
    console.log(
      `Liczba rekordów z przekroczonym rozmiarem way: ${oversizedWayCount}`
    );
    console.log(
      `Procent rekordów z przekroczonym rozmiarem way: ${(
        (oversizedWayCount / result.rows.length) *
        100
      ).toFixed(2)}%`
    );

    // Raport o duplikatach
    console.log(`\nRaport o duplikatach osm_id:`);
    console.log(`Liczba unikalnych osm_id: ${processedOsmIds.size}`);
    console.log(`Liczba duplikatów osm_id: ${duplicateOsmIds.size}`);
    if (duplicateOsmIds.size > 0) {
      console.log("Lista duplikatów osm_id:");
      duplicateOsmIds.forEach((id) => console.log(`- ${id}`));
    }

    // Statystyki typów budynków
    console.log("\nStatystyki typów budynków:");
    Object.entries(buildingTypeStats)
      .filter(([_, count]) => count > 0)
      .forEach(([type, count]) => {
        console.log(`${type}: ${count} rekordów`);
      });
  } catch (error) {
    console.error("Wystąpił błąd:", error);
  } finally {
    await pgClient.end();
  }
}

migrateData();
