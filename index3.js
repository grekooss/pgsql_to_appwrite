import { Client, Databases, Query } from "node-appwrite";
import { Client as GoogleMapsClient } from "@googlemaps/google-maps-services-js";
import fetch from "node-fetch";
import * as dotenv from "dotenv";
import * as path from "path";
import * as fs from "fs/promises";

// Załaduj zmienne środowiskowe
dotenv.config();

// Inicjalizacja klienta Appwrite
const appwrite = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

const databases = new Databases(appwrite);

// Inicjalizacja klienta Google Maps
const googleMapsClient = new GoogleMapsClient({});

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
    // Pobierz wszystkie dokumenty z kolekcji landings
    const response = await databases.listDocuments(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_COLLECTION_ID,
      [
        Query.isNotNull('center_point')
      ]
    );

    for (const landing of response.documents) {
      console.log(`Przetwarzanie landingu: ${landing.$id}`);
      
      // Parsuj współrzędne z center_point
      try {
        const coordinates = JSON.parse(landing.center_point);
        const [latitude, longitude] = coordinates;
        
        // Pobierz dane o miejscu z Google Places API
        const placeData = await getPlacesData(latitude, longitude, "");
        
        if (placeData && placeData.photos && placeData.photos.length > 0) {
          const photo = placeData.photos[0];
          const photoReference = photo.photo_reference;
          
          // Przygotuj ścieżkę do zapisu zdjęcia
          const outputDir = path.join(process.cwd(), 'processed_images');
          await fs.mkdir(outputDir, { recursive: true });
          const outputPath = path.join(outputDir, `${landing.$id}.jpg`);
          
          // Pobierz i zapisz zdjęcie
          const photoUrl = `https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photoreference=${photoReference}&key=${process.env.GOOGLE_MAPS_API_KEY}`;
          await downloadImage(photoUrl, outputPath);
          
          console.log(`Zapisano zdjęcie dla landingu ${landing.$id}`);
          
          // Zaktualizuj dokument w Appwrite z informacją o zdjęciu
          await databases.updateDocument(
            process.env.APPWRITE_DATABASE_ID,
            process.env.APPWRITE_COLLECTION_ID,
            landing.$id,
            {
              has_image: true,
              place_name: placeData.name,
              place_address: placeData.formatted_address,
              place_rating: placeData.rating,
              place_url: placeData.url
            }
          );
        } else {
          console.log(`Nie znaleziono zdjęć dla landingu ${landing.$id}`);
        }
      } catch (error) {
        console.error(`Błąd podczas przetwarzania współrzędnych dla landingu ${landing.$id}:`, error);
        continue;
      }
    }
    
    console.log('Zakończono przetwarzanie wszystkich landingów');
  } catch (error) {
    console.error('Wystąpił błąd podczas wykonywania skryptu:', error);
  }
}

main();
