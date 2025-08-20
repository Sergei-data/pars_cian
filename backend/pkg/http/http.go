package http

import (
	"net/http"
	"time"
)

var HttpClient = &http.Client{Timeout: 10 * time.Second}
var HttpLocalClient = &http.Client{Timeout: 3 * time.Second}
