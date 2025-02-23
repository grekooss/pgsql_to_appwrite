import { Client as GoogleMapsClient } from "@googlemaps/google-maps-services-js";
import * as dotenv from "dotenv";
import * as fsPromises from "fs/promises";
import { Client, Databases, InputFile, Query, Storage } from "node-appwrite";
import fetch from "node-fetch";
import * as os from "os";
import * as path from "path";
import sharp from "sharp";

// Załaduj zmienne środowiskowe
dotenv.config();

// Inicjalizacja klienta Appwrite
const client = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const databases = new Databases(client);
const storage = new Storage(client);

// Inicjalizacja klienta Google Maps
const googleMapsClient = new GoogleMapsClient({
  config: {
    params: {
      key: process.env.GOOGLE_MAPS_API_KEY,
    },
  },
});

async function getPlacesData(latitude, longitude, placeName) {
  try {
    console.log("Używany klucz API:", process.env.GOOGLE_MAPS_API_KEY);
    console.log("Szukam miejsca w lokalizacji:", {
      lat: parseFloat(latitude),
      lng: parseFloat(longitude),
    });

    const response = await googleMapsClient.placesNearby({
      params: {
        location: `${parseFloat(latitude)},${parseFloat(longitude)}`,
        radius: 1000,
      },
      timeout: 5000,
    });

    if (response.data.results.length > 0) {
      // Sortuj wszystkie wyniki po odległości od podanych współrzędnych
      const sortedResults = response.data.results.sort((a, b) => {
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

      const place = sortedResults[0]; // Weź najbliższe miejsce
      console.log("Znaleziono miejsce:", place.name);

      const placeDetails = await googleMapsClient.placeDetails({
        params: {
          place_id: place.place_id,
          fields: [
            "photos",
            "name",
            "formatted_address",
            "url",
            "website",
            "rating",
            "user_ratings_total",
          ],
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

async function downloadImage(url, osm_id, photoIndex) {
  try {
    const response = await fetch(url);
    const buffer = await response.arrayBuffer();
    const filename = `${osm_id}_${photoIndex}`;

    // Kompresja i zmiana rozmiaru zdjęcia
    const compressedImage = await sharp(Buffer.from(buffer))
      .resize(640, 480)
      .jpeg({ quality: 60 })
      .toBuffer();

    console.log(
      `Rozmiar zdjęcia po kompresji: ${compressedImage.length / 1024} KB`
    );

    // Zapisz bufor do pliku tymczasowego
    const tempPath = path.join(os.tmpdir(), `temp_${filename}`);
    await fsPromises.writeFile(tempPath, compressedImage);

    // Utwórz InputFile z pliku tymczasowego
    const file = InputFile.fromPath(tempPath, filename);

    // Przesyłamy zdjęcie do Appwrite Storage
    const result = await storage.createFile(
      process.env.APPWRITE_BUCKET_ID,
      filename,
      file
    );

    // Usuń plik tymczasowy
    await fsPromises.unlink(tempPath);

    return result.$id;
  } catch (error) {
    console.error("Błąd podczas pobierania/przesyłania zdjęcia:", error);
    throw error;
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
      photos: placeData.photos.map((photo) => ({
        photo_reference: photo.photo_reference,
        html_attributions: photo.html_attributions,
      })),
    };

    await fsPromises.writeFile(
      attributionsPath,
      JSON.stringify(attributions, null, 2),
      "utf8"
    );
  } catch (error) {
    console.error("Błąd podczas zapisywania atrybutów:", error);
  }
}

async function processPhotos(placeData, landing_id, landing) {
  if (!placeData || !placeData.photos || placeData.photos.length === 0) {
    console.log(`Nie znaleziono zdjęć dla landingu ${landing_id}`);
    return;
  }

  try {
    await saveAttributions(placeData, landing.osm_id);

    // Ograniczamy liczbę zdjęć do 3
    const photosToProcess = placeData.photos.slice(0, 3);

    for (const [index, photo] of photosToProcess.entries()) {
      // Numerujemy od 1
      const photoNumber = index + 1;
      const documentId = `${landing.osm_id}_${photoNumber}`;

      try {
        // Sprawdź czy dokument już istnieje
        try {
          await databases.getDocument(
            process.env.APPWRITE_DATABASE_ID,
            process.env.APPWRITE_PHOTOS_COLLECTION_ID,
            documentId
          );
          console.log(
            `Dokument ${documentId} już istnieje w kolekcji Photos, pomijam...`
          );
          continue;
        } catch (error) {
          // Jeśli dokument nie istnieje (404), kontynuujemy tworzenie
          if (error.code !== 404) {
            throw error;
          }
        }

        const photoUrl = `https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photo_reference=${photo.photo_reference}&key=${process.env.GOOGLE_MAPS_API_KEY}`;
        const fileId = await downloadImage(
          photoUrl,
          landing.osm_id,
          photoNumber
        );

        // Utwórz dokument w kolekcji Photos
        await databases.createDocument(
          process.env.APPWRITE_DATABASE_ID,
          process.env.APPWRITE_PHOTOS_COLLECTION_ID,
          documentId,
          {
            landings_id: landing_id,
            filename: fileId,
            html_attributions: photo.html_attributions[0] || "",
            google_maps_url: placeData.url || "",
            width: 640,
            height: 480,
          }
        );

        console.log(
          `Utworzono dokument ${documentId} w kolekcji Photos dla landingu ${landing_id}`
        );
      } catch (error) {
        console.error(
          `Błąd podczas przetwarzania zdjęcia ${photoNumber} dla landingu ${landing_id}:`,
          error
        );
        if (error.response) {
          console.error("Szczegóły błędu:", error.response);
        }
      }
    }
  } catch (error) {
    console.error(
      `Błąd podczas przetwarzania zdjęć dla landingu ${landing_id}:`,
      error
    );
    if (error.response) {
      console.error("Szczegóły błędu:", error.response);
    }
  }
}

async function main() {
  try {
    const landings = await databases.listDocuments(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_LANDINGS_COLLECTION_ID,
      [Query.limit(100)]
    );

    for (const landing of landings.documents) {
      console.log("Przetwarzanie landingu:", landing.$id);
      console.log("Dane landingu:", landing);

      // Parsuj współrzędne z center_point
      const coordinates = JSON.parse(landing.center_point);
      const [latitude, longitude] = coordinates;

      const placeData = await getPlacesData(latitude, longitude, landing.name);
      await processPhotos(placeData, landing.$id, landing);
    }
  } catch (error) {
    console.error("Błąd podczas przetwarzania landingów:", error);
  }
}

main();
