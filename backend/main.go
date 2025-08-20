package main

import (
	"log"
	"os"
	"pp/api"
	"pp/config"

	"github.com/gin-gonic/gin"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetPrefix("[INFO] ")

	c := config.Get()

	api.RunModel()

	if c.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.Default()

	router.Static("/assets", "../frontend/assets")
	router.LoadHTMLGlob("../frontend/*.html")

	router.GET("/", func(ctx *gin.Context) { ctx.HTML(200, "index.html", gin.H{"title": "Resale Market Predictions"}) })
	router.GET("/health", func(ctx *gin.Context) { ctx.Status(200) })
	// TODO: make API_KEY
	// router.GET("/parse", api.Parse)
	router.GET("/prediction", api.Prediction)

	router.Run(":" + c.Port)
}
