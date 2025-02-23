import { Client, Storage, Databases } from "node-appwrite";
import * as dotenv from "dotenv";

// Załaduj zmienne środowiskowe
dotenv.config();

// Inicjalizacja klienta Appwrite
const client = new Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const storage = new Storage(client);
const databases = new Databases(client);

async function deleteAllPhotos() {
  try {
    console.log("Rozpoczynam usuwanie zdjęć...");

    // Pobierz listę wszystkich plików z bucketa
    const files = await storage.listFiles(process.env.APPWRITE_BUCKET_ID);

    console.log(`Znaleziono ${files.total} plików do usunięcia`);

    // Usuń każdy plik
    for (const file of files.files) {
      try {
        await storage.deleteFile(process.env.APPWRITE_BUCKET_ID, file.$id);
        console.log(`Usunięto plik: ${file.$id}`);
      } catch (error) {
        console.error(`Błąd podczas usuwania pliku ${file.$id}:`, error);
      }
    }

    // Pobierz listę wszystkich dokumentów z kolekcji photos
    const documents = await databases.listDocuments(
      process.env.APPWRITE_DATABASE_ID,
      process.env.APPWRITE_PHOTOS_COLLECTION_ID
    );

    console.log(`Znaleziono ${documents.total} dokumentów do usunięcia`);

    // Usuń każdy dokument
    for (const doc of documents.documents) {
      try {
        await databases.deleteDocument(
          process.env.APPWRITE_DATABASE_ID,
          process.env.APPWRITE_PHOTOS_COLLECTION_ID,
          doc.$id
        );
        console.log(`Usunięto dokument: ${doc.$id}`);
      } catch (error) {
        console.error(`Błąd podczas usuwania dokumentu ${doc.$id}:`, error);
      }
    }

    console.log("Zakończono usuwanie wszystkich zdjęć i dokumentów");
  } catch (error) {
    console.error("Wystąpił błąd podczas usuwania zdjęć:", error);
  }
}

// Uruchom funkcję
deleteAllPhotos();
