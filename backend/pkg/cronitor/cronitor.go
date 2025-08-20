package cronitor

import (
	"fmt"
	"log"
	"net/http"
	"os"
	httpPkg "pp/pkg/http"
)

type Cronitor interface {
	JobStarted(jobName string) error
	JobCompleted(jobName string) error
	JobFailed(jobName string) error
}

type CronitorImpl struct {
	BaseUrl    string
	httpClient *http.Client
}

func NewCronitor() Cronitor {
	return &CronitorImpl{
		BaseUrl:    "https://cronitor.link/p/" + os.Getenv("CRONITOR_API_KEY") + "/",
		httpClient: httpPkg.HttpClient,
	}
}

type FakeCronitorImpl struct{}

func (c *FakeCronitorImpl) JobStarted(jobName string) error {
	log.Println(jobName + " (state = run)")
	return nil
}
func (c *FakeCronitorImpl) JobCompleted(jobName string) error {
	log.Println(jobName + " (state = complete)")
	return nil
}
func (c *FakeCronitorImpl) JobFailed(jobName string) error {
	log.Println(jobName + " (state = fail)")
	return nil
}

func NewFakeCronitor() Cronitor {
	return &FakeCronitorImpl{}
}

func (c *CronitorImpl) sendJobTelemetry(request string) error {
	resp, err := c.httpClient.Get(c.BaseUrl + request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *CronitorImpl) sanitizeJobName(jobName string) (string, error) {
	for _, c := range jobName {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return "", fmt.Errorf("invalid character '%c' in job name '%s'", c, jobName)
		}
	}
	return jobName, nil
}

// Sends a telemetry event to Cronitor indicating that a job has started running.
func (c *CronitorImpl) JobStarted(jobName string) error {
	name, err := c.sanitizeJobName(jobName)
	if err != nil {
		return err
	}
	return c.sendJobTelemetry(name + "?state=run")
}

// Sends a telemetry event to Cronitor indicating that a job has completed succesfully.
func (c *CronitorImpl) JobCompleted(jobName string) error {
	name, err := c.sanitizeJobName(jobName)
	if err != nil {
		return err
	}
	return c.sendJobTelemetry(name + "?state=complete")
}

// Sends a telemetry event to Cronitor indicating that a job has failed.
func (c *CronitorImpl) JobFailed(jobName string) error {
	name, err := c.sanitizeJobName(jobName)
	if err != nil {
		return err
	}
	return c.sendJobTelemetry(name + "?state=fail")
}
