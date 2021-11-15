package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"sync"
	"time"
)

type request struct {
	CustomerId int `json:"customerId" validate:"required"`
	Id         int `json:"id" validate:"required"`
}

type output struct {
	M map[int][]int
	Mut *sync.Mutex
}

func (o output) append(r request){
	o.Mut.Lock()
	defer o.Mut.Unlock()
	if _,ok := o.M[r.CustomerId] ; !ok {
		o.M[r.CustomerId] = make([]int,0)
	}
	o.M[r.CustomerId] = append(o.M[r.CustomerId],r.Id)
}

func (o output) String() string {
	buffer := bytes.NewBufferString("")
	for k,v := range o.M {
		_, err := fmt.Fprintf(buffer,"customerId : %d -> %v\n",k,v)
		if err != nil {
			continue
		}
	}
	return buffer.String()
}

type SQS struct {
	s *sqs.SQS
	linger int64
	queueUrl string
}

const (
	numberOfConsumers = 10
)

func main() {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String("eu-west-1")},
		Profile: "default",
	})

	if err != nil {
		log.Fatal(err)
	}


	s := SQS{
		sqs.New(sess),2,	"https://sqs.eu-west-1.amazonaws.com/236584826472/example.fifo",
	}

	channels := make([]chan struct{}, numberOfConsumers)

	o := output{M: make(map[int][]int),Mut: &sync.Mutex{}}

	for i := 0 ; i < numberOfConsumers; i++{
		channels[i] = make(chan struct{})
		go startReceive(s,channels[i],o)
	}

	time.Sleep(time.Second * 45)

	for _,c:= range channels {
		c <- struct {}{}
	}

	fmt.Printf("out : %s\n", o)

}

func startReceive(s SQS,c chan struct{},o output){
	Loop:
	for {
		select {
			case <- c:
				return
			default:
				attemptId := uuid.New().String()

				result, err := s.s.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueUrl: &s.queueUrl,
					MaxNumberOfMessages: aws.Int64(10),
					WaitTimeSeconds: &s.linger,
					ReceiveRequestAttemptId: &attemptId,
				})

				// read again - simulate network partition
				if rand.Intn(2) == 0 {
					result, err = s.s.ReceiveMessage(&sqs.ReceiveMessageInput{
						QueueUrl: &s.queueUrl,
						MaxNumberOfMessages: aws.Int64(10),
						WaitTimeSeconds: &s.linger,
						ReceiveRequestAttemptId: &attemptId,
					})
				}
				if err != nil {
					log.Printf("err : %s",err)
					continue Loop
				}
				for _,ms := range result.Messages {
					var r request
					if err := json.Unmarshal([]byte(*ms.Body),&r); err != nil {
						log.Printf("err : %s",err)
						continue
					}

					o.append(r)

					_, err := s.s.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      &s.queueUrl,
						ReceiptHandle: ms.ReceiptHandle,
					})
					if err != nil {
						log.Printf("err : %s",err)
						continue
					}
				}
		}
	}

}
