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

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type JobMessage struct {
	FilePath string `json:"file_path"`
	JobID    string `json:"job_id"`
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rag_jobs",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	r := gin.Default()

	uploadPath := "./temp-uploads"
	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		os.Mkdir(uploadPath, os.ModePerm)
	}

	r.POST("/upload", func(c *gin.Context) {
		file, err := c.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "No file uploaded"})
			return
		}

		jobID := fmt.Sprintf("%d", time.Now().Unix())
		filename := jobID + "_" + filepath.Base(file.Filename)
		fullPath := filepath.Join(uploadPath, filename)

		if err := c.SaveUploadedFile(file, fullPath); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
			return
		}

		absPath, _ := filepath.Abs(fullPath)
		messageBody := JobMessage{
			FilePath: absPath,
			JobID:    jobID,
		}

		body, err := json.Marshal(messageBody)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job"})
			return
		}

		err = ch.Publish(
			"",
			q.Name,
			false,
			false,
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

		c.JSON(http.StatusAccepted, gin.H{
			"status":  "queued",
			"job_id":  jobID,
			"message": "File uploaded and processing started.",
		})
	})

	log.Println("Server starting on http://localhost:8080")
	r.Run(":8080")
}
