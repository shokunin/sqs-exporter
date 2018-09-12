package collector

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"net/http"
	"strings"
)

type MetricHandler struct{}

type qStats struct {
	qName string
	stats *sqs.GetQueueAttributesOutput
}

func (h MetricHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queues := getQueues()
	for queue, attr := range queues {
		msgAvailable := *attr.Attributes["ApproximateNumberOfMessages"]
		msgDelayed := *attr.Attributes["ApproximateNumberOfMessagesDelayed"]
		msgNotvisible := *attr.Attributes["ApproximateNumberOfMessagesDelayed"]
		fmt.Fprintf(w, "sqs_messages_visible{queue_name=\"%s\"} %+v\n", queue, msgAvailable)
		fmt.Fprintf(w, "sqs_messages_delayed{queue_name=\"%s\"} %+v\n", queue, msgDelayed)
		fmt.Fprintf(w, "sqs_messages_not_visible{queue_name=\"%s\"} %+v\n", queue, msgNotvisible)
	}
}

func getQueueName(url string) (queueName string) {
	queue := strings.Split(url, "/")
	queueName = queue[len(queue)-1]
	return
}

func getQueueStats(client *sqs.SQS, jobs <-chan string, results chan<- qStats, done chan<- bool) {
	for url := range jobs {
		params := &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(url),
			AttributeNames: []*string{
				aws.String("ApproximateNumberOfMessages"),
				aws.String("ApproximateNumberOfMessagesDelayed"),
				aws.String("ApproximateNumberOfMessagesNotVisible"),
			},
		}
		s, _ := client.GetQueueAttributes(params)
		q := getQueueName(url)
		results <- qStats{qName: q, stats: s}
	}
	done <- true

}

func getQueues() (queues map[string]*sqs.GetQueueAttributesOutput) {
	workerCount := 20
	sess := session.Must(session.NewSession(&aws.Config{
		MaxRetries: aws.Int(3),
	}))
	client := sqs.New(sess)
	result, err := client.ListQueues(nil)
	if err != nil {
		log.Fatal("Error ", err)
	}

	jobs := make(chan string, len(result.QueueUrls))
	results := make(chan qStats, len(result.QueueUrls))
	done := make(chan bool)

	queues = make(map[string]*sqs.GetQueueAttributesOutput)

	if result.QueueUrls == nil {
		log.Println("Couldnt find any queues in region:", *sess.Config.Region)
	}

	for w := 0; w <= workerCount; w++ {
		go getQueueStats(client, jobs, results, done)
	}

	for _, urls := range result.QueueUrls {
		jobs <- *aws.String(*urls)
	}
	for a := 0; a < len(result.QueueUrls); a++ {
		r := <-results
		queues[r.qName] = r.stats
	}
	return queues
}
