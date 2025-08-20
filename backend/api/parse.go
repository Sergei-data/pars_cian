package api

import (
	"bytes"
	"errors"
	"log"
	"os/exec"
	"pp/config"
	"pp/pkg/pipeline"
	"time"

	"github.com/gin-gonic/gin"
)

func Parse(ctx *gin.Context) {
	if ctx.Query("stop") == "true" {
		stopParse()
		return
	}
	runParse()
}

func stopParse() {
	stopping := process.TryStop()
	log.Println(stopping)
}

func runParse() {
	starting := process.TryStart()
	log.Println(starting)
}

var process *pipeline.AtomicProcess[*ParseCommand] = pipeline.NewAtomicProcess(&ParseCommand{})
var lastSuccessfullParseTime time.Time = time.Unix(0, 0)

type ParseCommand struct {
	cmd *exec.Cmd
	out *bytes.Buffer
}

var _ pipeline.Command = &ParseCommand{}
var _ pipeline.Command = (*ParseCommand)(nil)

func (c *ParseCommand) Interrupt() {
	if err := c.cmd.Process.Kill(); err != nil {
		log.Println(err)
	}

	lastSuccessfullParseTime = time.Unix(0, 0)
}

func (c *ParseCommand) Run(e pipeline.ExecutionContext) {
	if err := c.run(e); err != nil {
		log.Println(err)
		if err := config.Get().Cronitor.JobFailed("parsing"); err != nil {
			log.Println(err)
		}
	}
}

func (c *ParseCommand) run(e pipeline.ExecutionContext) error {
	if err := config.Get().Cronitor.JobStarted("parsing"); err != nil {
		log.Println(err)
	}

	e.AllowInterruption(true)

	if !e.AllowInterruption(false) {
		return errors.New("interrupted")
	}

	if err := c.start(); err != nil {
		return err
	}

	e.AllowInterruption(true)

	if err := c.cmd.Wait(); err != nil {
		return err
	}
	log.Println(c.out.String())

	if !e.AllowInterruption(false) {
		return errors.New("interrupted")
	}

	lastSuccessfullParseTime = time.Now()

	if err := config.Get().Cronitor.JobCompleted("parsing"); err != nil {
		log.Println(err)
	}

	return nil
}

func (c *ParseCommand) start() error {
	if time.Since(lastSuccessfullParseTime) < 48*time.Hour {
		return errors.New("last successful parse was less than 48 hours ago")
	}

	c.cmd = exec.Command("pnpm", "start")
	c.cmd.Dir = "../rmp-parsers/domclick"
	c.out = &bytes.Buffer{}
	c.cmd.Stdout = c.out
	c.cmd.Stderr = c.out
	return c.cmd.Start()
}
