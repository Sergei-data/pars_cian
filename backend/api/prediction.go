package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"os/exec"
	httpPkg "pp/pkg/http"
	"pp/pkg/pipeline"
	"strconv"

	"github.com/gin-gonic/gin"
)

type ModelPrediction struct {
	Rf  string
	Xgb string
}

func Prediction(ctx *gin.Context) {
	prediction := ModelPrediction{}
	err := getPrediction(ctx, &prediction)
	if err != nil {
		log.Println(err)
		ctx.HTML(422, "bad_prediction_request.html", nil)
		return
	}

	ctx.HTML(200, "prediction.html", gin.H{"rf": prediction.Rf, "xgb": prediction.Xgb})
}

func getPrediction(ctx *gin.Context, prediction interface{}) error {
	city, err := strconv.Atoi(ctx.Query("city"))
	if err != nil {
		return err
	}
	if city < 0 {
		city = 0
	}
	if city > 3 {
		city = 3
	}
	room_count, err := strconv.Atoi(ctx.Query("room_count"))
	if err != nil {
		return err
	}
	floor, err := strconv.Atoi(ctx.Query("floor"))
	if err != nil {
		return err
	}
	total_floors, err := strconv.Atoi(ctx.Query("total_floors"))
	if err != nil {
		return err
	}
	area, err := strconv.ParseFloat(ctx.Query("area"), 64)
	if err != nil {
		return err
	}
	kitchen_area, err := strconv.ParseFloat(ctx.Query("kitchen_area"), 64)
	if err != nil {
		return err
	}
	living_area, err := strconv.ParseFloat(ctx.Query("living_area"), 64)
	if err != nil {
		return err
	}
	ceiling_height, err := strconv.ParseFloat(ctx.Query("ceiling_height"), 64)
	if err != nil {
		return err
	}
	repair_type, err := strconv.Atoi(ctx.Query("repair_type"))
	if err != nil {
		return err
	}
	build_year, err := strconv.Atoi(ctx.Query("build_year"))
	if err != nil {
		return err
	}
	heating_type, err := strconv.Atoi(ctx.Query("heating_type"))
	if err != nil {
		return err
	}

	resp, err := httpPkg.HttpLocalClient.Get("http://127.0.0.1:4567/" +
		"?city=" + strconv.Itoa(city) +
		"&room_count=" + strconv.Itoa(room_count) +
		"&floor=" + strconv.Itoa(floor) +
		"&total_floors=" + strconv.Itoa(total_floors) +
		"&area=" + strconv.FormatFloat(area, 'f', -1, 64) +
		"&kitchen_area=" + strconv.FormatFloat(kitchen_area, 'f', -1, 64) +
		"&living_area=" + strconv.FormatFloat(living_area, 'f', -1, 64) +
		"&ceiling_height=" + strconv.FormatFloat(ceiling_height, 'f', -1, 64) +
		"&repair_type=" + strconv.Itoa(repair_type) +
		"&build_year=" + strconv.Itoa(build_year) +
		"&heating_type=" + strconv.Itoa(heating_type))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(prediction); err != nil {
		return err
	}
	return nil
}

func RunModel() {
	starting := modelProcess.TryStart()
	log.Println(starting)
}

var modelProcess *pipeline.AtomicProcess[*ModelCommand] = pipeline.NewAtomicProcess(&ModelCommand{})

type ModelCommand struct {
	cmd *exec.Cmd
	out *bytes.Buffer
}

var _ pipeline.Command = &ModelCommand{}
var _ pipeline.Command = (*ModelCommand)(nil)

func (c *ModelCommand) Interrupt() {
	if err := c.cmd.Process.Kill(); err != nil {
		log.Println(err)
	}
}

func (c *ModelCommand) Run(e pipeline.ExecutionContext) {
	if err := c.run(e); err != nil {
		log.Println(err)
	}
}

func (c *ModelCommand) run(e pipeline.ExecutionContext) error {
	e.AllowInterruption(true)

	if !e.AllowInterruption(false) {
		return errors.New("interrupted")
	}

	if err := c.start(); err != nil {
		return err
	}

	e.AllowInterruption(true)

	if err := c.cmd.Wait(); err != nil {
		log.Println(c.out.String())
		return err
	}
	log.Println(c.out.String())

	if !e.AllowInterruption(false) {
		return errors.New("interrupted")
	}

	return nil
}

func (c *ModelCommand) start() error {
	c.cmd = exec.Command("python3", "model.py")
	c.cmd.Dir = "../rmp-model"
	c.out = &bytes.Buffer{}
	c.cmd.Stdout = c.out
	c.cmd.Stderr = c.out
	return c.cmd.Start()
}
