package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"os"
	"time"
)

func (w *Worker) Run() {
	verboseLogger.Printf("[%d] initializing \n", w.WorkerId)

	queue := make(chan [2]string)
	
	cid := w.WorkerId
	qos := w.Qos
	
	t := randomSource.Int31()

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	subscriberClientId := fmt.Sprintf(subscriberClientIdTemplate, hostname, w.WorkerId, t)

	verboseLogger.Printf("[%d] topic=%s subscriberClientId=%s\n", cid, w.TopicName, subscriberClientId)

	subscriberOptions := mqtt.NewClientOptions().SetClientID(subscriberClientId).SetUsername(w.Username).SetPassword(w.Password).SetKeepAlive(30).AddBroker(w.BrokerUrl)

	subscriberOptions.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		queue <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	
	subscriber := mqtt.NewClient(subscriberOptions)

	verboseLogger.Printf("----[%d]--- connecting subscriber [%s]---- \n", w.WorkerId,w.TopicName)
	if token := subscriber.Connect(); token.Wait() && token.Error() != nil {
		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        "ConnectFailed",
			Error:        true,
			ErrorMessage: token.Error(),
		}

		return
	}
	
	time.Sleep(3 * time.Second)
	
	verboseLogger.Printf("----[%d] subscribing to topic [%s]----\n", w.WorkerId,w.TopicName)
	if token := subscriber.Subscribe(w.TopicName, qos, nil); token.WaitTimeout(opTimeout) && token.Error() != nil {
		resultChan <- Result{
			WorkerId:     w.WorkerId,
			Event:        "SubscribeFailed",
			Error:        true,
			ErrorMessage: token.Error(),
		}

		return
	}
	
	time.Sleep(3 * time.Second)
	
	defer func() {
		if token := subscriber.Unsubscribe(w.TopicName); token.WaitTimeout(opTimeout) && token.Error() != nil {
			fmt.Println(token.Error())
			os.Exit(1)
		}

		subscriber.Disconnect(5)
		verboseLogger.Printf("---[%d] unsubscribe\n", w.WorkerId)
		
	}()

	verboseLogger.Printf("----------Result -----------\n")

	timeout := make(chan bool, 1)
	stopWorker := false
	receivedCount := 0
	publishedCount := 0


	t0 := time.Now()
	publishTime := time.Since(t0)

	go func() {
		time.Sleep(w.Timeout)
		timeout <- true
	}()

	for receivedCount < w.Nmessages && !stopWorker {
		select {
		case <-queue:
			receivedCount++

			verboseLogger.Printf("[%d] %d/%d received\n", w.WorkerId, receivedCount, w.Nmessages)
			if receivedCount == w.Nmessages {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             "Completed",
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			} else {
				resultChan <- Result{
					WorkerId:          w.WorkerId,
					Event:             "ProgressReport",
					PublishTime:       publishTime,
					ReceiveTime:       time.Since(t0),
					MessagesReceived:  receivedCount,
					MessagesPublished: publishedCount,
				}
			}
		case <-timeout:
			verboseLogger.Printf("[%d] timeout!!\n", cid)
			stopWorker = true

			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "TimeoutExceeded",
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             true,
			}
		case <-abortChan:
			verboseLogger.Printf("[%d] received abort signal", w.WorkerId)
			stopWorker = true

			resultChan <- Result{
				WorkerId:          w.WorkerId,
				Event:             "Aborted",
				PublishTime:       publishTime,
				MessagesReceived:  receivedCount,
				MessagesPublished: publishedCount,
				Error:             false,
			}
		}
	}

	verboseLogger.Printf("[%d] worker finished\n", w.WorkerId)
}
