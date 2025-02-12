import pg from 'pg';
import { Client, Databases, ID } from 'node-appwrite';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

// Konfiguracja środowiska
const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: join(__dirname, '.env') });

// Konfiguracja PostgreSQL
const pgClient = new pg.Client({
    host: process.env.PGHOST,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE,
    port: process.env.PGPORT
});

// Konfiguracja Appwrite
const appwrite = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

const databases = new Databases(appwrite);

// Mapowanie typów PostgreSQL na typy Appwrite
const pgToAppwriteType = {
    'integer': 'integer',
    'bigint': 'integer',
    'character varying': 'string',
    'text': 'string',
    'boolean': 'boolean',
    'timestamp with time zone': 'datetime',
    'timestamp without time zone': 'datetime',
    'date': 'datetime',
    'double precision': 'double',
    'real': 'double',
    'numeric': 'double',
    'json': 'string',
    'jsonb': 'string'
};

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
            console.log('Kolekcja już istnieje, usuwam...');
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

        console.log('Utworzono nową kolekcję');

        // Tworzenie atrybutów dla każdej kolumny
        for (const col of columns) {
            const colName = col.replace(/"/g, '');
            
            try {
                if (colName === 'osm_id') {
                    await databases.createStringAttribute(
                        process.env.APPWRITE_DATABASE_ID,
                        process.env.APPWRITE_COLLECTION_ID,
                        colName,
                        255,
                        true
                    );
                } else if (colName === 'way_area') {
                    await databases.createFloatAttribute(
                        process.env.APPWRITE_DATABASE_ID,
                        process.env.APPWRITE_COLLECTION_ID,
                        colName,
                        false
                    );
                } else if (colName === 'center_point') {
                    await databases.createStringAttribute(
                        process.env.APPWRITE_DATABASE_ID,
                        process.env.APPWRITE_COLLECTION_ID,
                        colName,
                        255,
                        false
                    );
                } else if (colName === 'way') {
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
        await new Promise(resolve => setTimeout(resolve, 2000));
        
    } catch (error) {
        console.error('Błąd podczas tworzenia kolekcji:', error);
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
        console.log('Połączono z PostgreSQL');

        // Nazwa tabeli do migracji
        const tableName = 'planet_osm_polygon';
        
        let oversizedWayCount = 0;
        
        // Wybrane kolumny do migracji (z poprawnymi nazwami SQL)
        const selectedColumns = [
            'osm_id',
            'name',
            'building',
            'amenity',
            'landuse',
            'natural',
            'way_area',
            'center_point',
            'way'
        ].map(col => `"${col}"`);

        // Pobierz dane z PostgreSQL z limitem
        const query = `
            SELECT 
                ${selectedColumns.filter(col => col !== '"center_point"' && col !== '"way"').join(', ')},
                ARRAY[ST_Y(ST_Transform(ST_Centroid("way"), 4326)), ST_X(ST_Transform(ST_Centroid("way"), 4326))] as "center_point",
                ST_AsGeoJSON(ST_Transform(ST_RemoveRepeatedPoints(ST_Simplify("way", 0.1)), 4326)) as "way"
            FROM ${tableName}
            WHERE "name" IS NOT NULL 
               OR "building" IS NOT NULL 
               OR "amenity" IS NOT NULL
               OR "landuse" IS NOT NULL
               OR "natural" IS NOT NULL
            LIMIT 100
        `;
        const result = await pgClient.query(query);
        console.log(`\nZnaleziono ${result.rows.length} rekordów do migracji`);
        
        // Utwórz kolekcję w Appwrite
        await createAppwriteCollection(tableName, selectedColumns);
        console.log('Utworzono/zaktualizowano kolekcję w Appwrite');

        // Migracja każdego rekordu do Appwrite
        for (const row of result.rows) {
            try {
                // Konwertuj liczby na stringi, ponieważ Appwrite może mieć problemy z dużymi liczbami
                const wayData = row.way ? JSON.parse(row.way) : null;
                const wayPoints = wayData ? wayData.coordinates[0].map(point => [point[1], point[0]]) : null;
                const wayJson = wayPoints ? JSON.stringify(wayPoints) : null;
                
                const isWayOversized = wayJson && wayJson.length > 100000;
                if (isWayOversized) {
                    oversizedWayCount++;
                }

                const documentData = {
                    ...row,
                    osm_id: row.osm_id.toString(),
                    way_area: row.way_area ? parseFloat(row.way_area) : null,
                    center_point: row.center_point ? JSON.stringify(row.center_point) : null,
                    way: isWayOversized ? null : wayJson
                };

                await databases.createDocument(
                    process.env.APPWRITE_DATABASE_ID,
                    process.env.APPWRITE_COLLECTION_ID,
                    ID.unique(),
                    documentData
                );
                console.log(`Zmigrowano rekord: ${documentData.osm_id}`);
            } catch (error) {
                console.error('Błąd podczas migracji rekordu:', error);
            }
        }

        console.log('\nRaport z migracji:');
        console.log(`Całkowita liczba przetworzonych rekordów: ${result.rows.length}`);
        console.log(`Liczba rekordów z przekroczonym rozmiarem way: ${oversizedWayCount}`);
        console.log(`Procent rekordów z przekroczonym rozmiarem way: ${((oversizedWayCount / result.rows.length) * 100).toFixed(2)}%`);

    } catch (error) {
        console.error('Wystąpił błąd:', error);
    } finally {
        await pgClient.end();
    }
}

migrateData();
