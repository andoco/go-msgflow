package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andoco/go-msgflow/step"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)

	step1 := step.New("step1", newHello("step1"))
	//step1 := step.New("step1", newSqsReceive(os.Getenv("SQS_URL")))
	step2 := step.New("step2", newDelay())
	step3 := step.New("step3", newLog("step3"))
	step4 := step.New("step4", newErr())
	step5 := step.New("step5", newLogErr())

	step1.ConnectTo(step2)
	step2.ConnectTo(step3)
	step3.ConnectTo(step4)
	step4.ErrorTo(step5)

	step1.Start()
	step2.Start()
	step3.Start()
	step4.Start()
	step5.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c
}

func newHello(id string) step.F {
	return func(in interface{}) (interface{}, error) {
		return fmt.Sprintf("hello for %s", id), nil
	}
}

func newDelay() step.F {
	return func(in interface{}) (interface{}, error) {
		time.Sleep(time.Second * 3)
		return in, nil
	}
}

func newLog(id string) step.F {
	return func(in interface{}) (interface{}, error) {
		log.WithField("stepId", id).WithField("in", in).Debug("received input value")
		return nil, nil
	}
}

func newErr() step.F {
	return func(in interface{}) (interface{}, error) {
		log.Debug("HERE")
		return nil, fmt.Errorf("error from step")
	}
}

func newLogErr() step.F {
	return func(in interface{}) (interface{}, error) {
		log.WithField("in", in).Error("Received item to be handled as a failure")
		return nil, nil
	}
}

type Step interface {
	Start()
	Stop()
}

func newSqsReceive(url string) step.F {
	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		log.Fatal(err)
	}

	svc := sqs.New(sess)

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(url),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(5),
	}

	return func(in interface{}) (interface{}, error) {
		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("recieved message: %v", resp)

		if len(resp.Messages) == 0 {
			return nil, nil
		}

		return resp.Messages[0], nil
	}
}
