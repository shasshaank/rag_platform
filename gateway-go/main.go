package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
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
	// Try loading .env from parent directory
	_ = godotenv.Load("../.env")

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp.Dial(rabbitURL)
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

	// AWS S3 Initialization
	cfg, err := config.LoadDefaultConfig(context.TODO())
	failOnError(err, "Failed to load AWS configuration")
	s3Client := s3.NewFromConfig(cfg)
	bucketName := os.Getenv("AWS_S3_BUCKET")
	if bucketName == "" {
		log.Println("WARNING: AWS_S3_BUCKET is not set. File uploads to S3 will fail.")
	}

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	r.POST("/upload", func(c *gin.Context) {
		file, err := c.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "No file uploaded"})
			return
		}

		jobID := fmt.Sprintf("%d", time.Now().Unix())
		filename := jobID + "_" + filepath.Base(file.Filename)
		s3Key := "uploads/" + filename

		// Open uploaded file
		src, err := file.Open()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open uploaded file"})
			return
		}
		defer src.Close()

		// Stream directly to S3
		_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
			Body:   src,
		})
		if err != nil {
			log.Printf("S3 Upload Error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to upload file to Cloud Storage"})
			return
		}

		// Use an s3:// URI as the file path
		s3Path := fmt.Sprintf("s3://%s/%s", bucketName, s3Key)

		messageBody := JobMessage{
			FilePath: s3Path,
			JobID:    jobID,
		}

		body, err := json.Marshal(messageBody)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize job message"})
			return
		}

		// Re-obtain channel just in case of stale connections
		publishCh, err := conn.Channel()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open RabbitMQ channel"})
			return
		}
		defer publishCh.Close()

		err = publishCh.Publish(
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

		log.Printf(" [x] Sent Job to Queue via S3: %s", s3Path)

		c.JSON(http.StatusAccepted, gin.H{
			"status":  "queued",
			"job_id":  jobID,
			"message": "File uploaded to AWS S3 and processing started.",
		})
	})

	log.Println("Server starting on http://localhost:8080")
	r.Run(":8081")
}
