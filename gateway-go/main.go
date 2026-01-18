package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/streadway/amqp"
)

// Helper function to handle errors
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Define the JSON structure for the message we send to the Worker
type JobMessage struct {
	FilePath string `json:"file_path"`
	JobID    string `json:"job_id"`
}

func main() {
	// ---------------------------------------------------------
	// 1. SETUP RABBITMQ CONNECTION
	// ---------------------------------------------------------
	// Connect to the RabbitMQ container running on localhost:5672
	// Default user/pass for the docker image is guest/guest
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare the queue named "rag_jobs" to ensure it exists
	q, err := ch.QueueDeclare(
		"rag_jobs", // name
		true,       // durable (messages survive if RabbitMQ restarts)
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// ---------------------------------------------------------
	// 2. SETUP GIN SERVER
	// ---------------------------------------------------------
	r := gin.Default()

	// Create the temp folder if it doesn't exist
	uploadPath := "./temp-uploads"
	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		os.Mkdir(uploadPath, os.ModePerm)
	}

	// ---------------------------------------------------------
	// 3. DEFINE THE UPLOAD ROUTE
	// ---------------------------------------------------------
	r.POST("/upload", func(c *gin.Context) {
		// A. Receive the file from the request
		file, err := c.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "No file uploaded"})
			return
		}

		// B. Save the file locally
		// Create a unique filename (e.g., "123_contract.pdf") to prevent overwrites
		jobID := fmt.Sprintf("%d", time.Now().Unix())
		filename := jobID + "_" + filepath.Base(file.Filename)
		fullPath := filepath.Join(uploadPath, filename)

		if err := c.SaveUploadedFile(file, fullPath); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
			return
		}

		// C. Prepare the message for RabbitMQ
		// We need the absolute path so the Python worker can find it later
		absPath, _ := filepath.Abs(fullPath)
		messageBody := JobMessage{
			FilePath: absPath,
			JobID:    jobID,
		}
		
		// Convert struct to JSON bytes
		body, err := json.Marshal(messageBody)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job"})
			return
		}

		// D. Publish the message to the queue
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key (queue name)
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         body,
			})
		
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish message"})
			return
		}

		log.Printf(" [x] Sent Job: %s", body)

		// E. Return success to the user
		c.JSON(http.StatusAccepted, gin.H{
			"status":  "queued",
			"job_id":  jobID,
			"message": "File uploaded and processing started.",
		})
	})

	// Start the server on port 8080
	log.Println("Server starting on http://localhost:8080")
	r.Run(":8080")
}