package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WeatherData struct {
	City        string    `bson:"city" json:"city"`
	Description string    `bson:"description" json:"description"`
	Temp        float64   `bson:"temp" json:"temp"`
	LastUpdated time.Time `bson:"last_updated" json:"last_updated"`
}

type weatherjson struct {
	Weather []struct {
		Description string `json:"description"`
	} `json:"weather"`

	Main struct {
		Temp float64 `json:"temp"`
	} `json:"main"`

	Name string `json:"name"`
}

var weatherCollection *mongo.Collection

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	MONGO_URI := os.Getenv("MONGO_URI")
	BASE_URL := os.Getenv("BASE_URL")
	API_KEY := os.Getenv("API_KEY")

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(MONGO_URI))
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	error := client.Ping(ctx, nil)
	if error != nil {
		log.Fatal("Failed to ping MongoDB:", error)
	}

	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal("Failed to disconnect MongoDB:", err)
		}
	}()

	weatherCollection = client.Database("weatherdb").Collection("weather")

	http.HandleFunc("/weather", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getWeatherHandler(w, r)
		case http.MethodPut:
			putWeatherHandler(w, r, BASE_URL, API_KEY)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getWeatherHandler(w http.ResponseWriter, r *http.Request) {
	city := r.URL.Query().Get("city")
	if city == "" {
		http.Error(w, "City parameter is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var weather WeatherData
	err := weatherCollection.FindOne(ctx, bson.M{"city": city}).Decode(&weather)
	if err != nil {
		http.Error(w, "Weather data not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(weather)
}

func putWeatherHandler(w http.ResponseWriter, r *http.Request, baseURL, apiKey string) {
	var requestBody struct {
		City string `json:"city"`
	}
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	city := requestBody.City
	if city == "" {
		http.Error(w, "City is required", http.StatusBadRequest)
		return
	}

	// Fetch weather data from OpenWeather API
	searchURL := fmt.Sprintf("%v?appid=%s&q=%s", baseURL, apiKey, city)
	response, err := http.Get(searchURL)
	if err != nil {
		http.Error(w, "Failed to fetch weather data", http.StatusInternalServerError)
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		http.Error(w, "Failed to fetch weather data from API", http.StatusInternalServerError)
		return
	}

	weatherBytes, _ := io.ReadAll(response.Body)
	var weatherAPIResponse weatherjson
	if err := json.Unmarshal(weatherBytes, &weatherAPIResponse); err != nil {
		http.Error(w, "Failed to parse weather data", http.StatusInternalServerError)
		return
	}

	// Prepare the data for MongoDB
	weatherData := WeatherData{
		City:        weatherAPIResponse.Name,
		Description: weatherAPIResponse.Weather[0].Description,
		Temp:        weatherAPIResponse.Main.Temp - 273.15,
		LastUpdated: time.Now(),
	}

	// Upsert data into MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := bson.M{"city": weatherData.City}
	update := bson.M{"$set": weatherData}
	opts := options.Update().SetUpsert(true)

	_, err = weatherCollection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		http.Error(w, "Failed to update weather data", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(weatherData)
}
