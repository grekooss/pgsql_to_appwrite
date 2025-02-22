import { Client } from "@googlemaps/google-maps-services-js";
import pg from "pg";
import fetch from "node-fetch";
import * as dotenv from "dotenv";
import * as path from "path";
import * as fs from "fs/promises";

// Załaduj zmienne środowiskowe
dotenv.config();

// Konfiguracja klienta PostgreSQL
const pool = new pg.Pool({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
});

// Inicjalizacja klienta Google Maps
const googleMapsClient = new Client({});

async function getPlacesData(latitude, longitude, placeName) {
  try {
    console.log("Używany klucz API:", process.env.GOOGLE_MAPS_API_KEY);

    // Lista typów miejsc do sprawdzenia
    const placeTypes = [
      "stadium", // stadion
      "park", // park (może zawierać boiska)
      "school", // szkoła (często ma boiska)
      "gym", // siłownia/kompleks sportowy
      "sports_complex", // kompleks sportowy
    ];

    let allResults = [];

    // Wykonaj wyszukiwanie dla każdego typu miejsca
    for (const type of placeTypes) {
      const response = await googleMapsClient.placesNearby({
        params: {
          location: { lat: latitude, lng: longitude },
          radius: 5,
          keyword: `${placeName} boisko stadion soccer football`,
          type: type,
          key: process.env.GOOGLE_MAPS_API_KEY,
        },
        timeout: 5000,
      });

      if (response.data.results.length > 0) {
        allResults = allResults.concat(response.data.results);
      }
    }

    if (allResults.length > 0) {
      // Sortuj wszystkie wyniki po odległości od podanych współrzędnych
      const sortedResults = allResults.sort((a, b) => {
        const distA = Math.sqrt(
          Math.pow(a.geometry.location.lat - latitude, 2) +
            Math.pow(a.geometry.location.lng - longitude, 2)
        );
        const distB = Math.sqrt(
          Math.pow(b.geometry.location.lat - latitude, 2) +
            Math.pow(b.geometry.location.lng - longitude, 2)
        );
        return distA - distB;
      });

      // Usuń duplikaty na podstawie place_id
      const uniqueResults = sortedResults.filter(
        (result, index, self) =>
          index === self.findIndex((t) => t.place_id === result.place_id)
      );

      const place = uniqueResults[0]; // Weź najbliższe miejsce
      console.log(
        "Znaleziono miejsce:",
        place.name,
        `(typ: ${place.types.join(", ")})`
      );

      const placeDetails = await googleMapsClient.placeDetails({
        params: {
          place_id: place.place_id,
          fields: ["photos", "name", "formatted_address", "url", "website", "rating", "user_ratings_total"],
          key: process.env.GOOGLE_MAPS_API_KEY,
        },
      });

      return placeDetails.data.result;
    }
    return null;
  } catch (error) {
    console.error("Błąd podczas pobierania danych z Google Places:", error);
    return null;
  }
}

async function downloadImage(url, outputPath) {
  try {
    const response = await fetch(url);
    const buffer = await response.buffer();
    await fs.writeFile(outputPath, buffer);
    console.log("Zapisano zdjęcie:", outputPath);
  } catch (error) {
    console.error("Błąd podczas pobierania zdjęcia:", error);
  }
}

async function saveAttributions(placeData, osm_id) {
  try {
    const attributionsPath = path.join(
      process.cwd(),
      "processed_images",
      `${osm_id}_attributions.json`
    );

    const attributions = {
      place_name: placeData.name,
      formatted_address: placeData.formatted_address,
      google_maps_url: placeData.url,
      website: placeData.website || null,
      rating: placeData.rating || null,
      user_ratings_total: placeData.user_ratings_total || null,
      photos: placeData.photos.map(photo => ({
        photo_reference: photo.photo_reference,
        html_attributions: photo.html_attributions,
        height: photo.height,
        width: photo.width
      }))
    };

    await fs.writeFile(
      attributionsPath,
      JSON.stringify(attributions, null, 2),
      "utf8"
    );
    console.log("Zapisano informacje o atrybucji:", attributionsPath);
  } catch (error) {
    console.error("Błąd podczas zapisywania informacji o atrybucji:", error);
  }
}

async function main() {
  try {
    console.log("Łączenie z bazą danych...");
    const client = await pool.connect();
    console.log("Połączono z bazą danych.");

    console.log("Wykonywanie zapytania SQL...");
    const result = await client.query(
      `SELECT osm_id, 
              ST_Y(ST_Transform(ST_Centroid(way), 4326)) as latitude,
              ST_X(ST_Transform(ST_Centroid(way), 4326)) as longitude,
              name
       FROM planet_osm_polygon 
       WHERE sport = 'soccer' 
       AND name IS NOT NULL 
       LIMIT 10`
    );

    console.log("Znaleziono", result.rows.length, "boisk piłkarskich:\n");

    // Utwórz folder na zdjęcia jeśli nie istnieje
    await fs.mkdir(path.join(process.cwd(), "processed_images"), {
      recursive: true,
    });

    // Przetwórz każde boisko
    for (const row of result.rows) {
      console.log(
        `- ${row.name} (${row.latitude}, ${row.longitude}) [OSM ID: ${row.osm_id}]`
      );
      console.log("Szukanie miejsca w Google Places...");
      const placeData = await getPlacesData(
        row.latitude,
        row.longitude,
        row.name
      );

      if (placeData && placeData.photos) {
        // Zapisz informacje o atrybucji
        await saveAttributions(placeData, row.osm_id);

        // Pobierz maksymalnie 3 zdjęcia
        const photosToDownload = placeData.photos.slice(0, 3);
        for (let i = 0; i < photosToDownload.length; i++) {
          const photo = photosToDownload[i];
          const photoUrl = `https://maps.googleapis.com/maps/api/place/photo?maxwidth=1600&photo_reference=${
            photo.photo_reference
          }&key=${process.env.GOOGLE_MAPS_API_KEY}`;
          const outputPath = path.join(
            process.cwd(),
            "processed_images",
            `${row.osm_id}_${i + 1}.jpg`
          );
          await downloadImage(photoUrl, outputPath);
        }
      } else {
        console.log("Nie znaleziono zdjęć dla tego miejsca");
      }
      console.log("---\n");
    }

    client.release();
  } catch (error) {
    console.error("Błąd:", error);
  } finally {
    await pool.end();
  }
}

main();
