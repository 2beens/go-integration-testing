package kafka

import (
	"context"
	"fmt"
	"time"

	segkafka "github.com/segmentio/kafka-go"
)

// WaitForConsumerGroupReady waits for a Kafka consumer group to be ready on a given topic.
func WaitForConsumerGroupReady(
	parentCtx context.Context,
	broker, groupID, topic string,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	client := &segkafka.Client{
		Addr:    segkafka.TCP(broker),
		Timeout: 2 * time.Second,
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastState := "unknown"
	var lastErr error

	for {
		resp, err := client.DescribeGroups(ctx, &segkafka.DescribeGroupsRequest{
			GroupIDs: []string{groupID},
		})
		if err == nil && consumerGroupHasTopicAssignment(resp, topic) {
			return nil
		}
		if err != nil {
			lastErr = err
		} else if len(resp.Groups) > 0 {
			lastState = resp.Groups[0].GroupState
			lastErr = resp.Groups[0].Error
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for kafka consumer group %q on topic %q: state=%q err=%v", groupID, topic, lastState, lastErr)
		case <-ticker.C:
		}
	}
}

func consumerGroupHasTopicAssignment(resp *segkafka.DescribeGroupsResponse, topic string) bool {
	if resp == nil {
		return false
	}

	for _, group := range resp.Groups {
		if group.Error != nil {
			continue
		}
		for _, member := range group.Members {
			for _, assignedTopic := range member.MemberAssignments.Topics {
				if assignedTopic.Topic == topic && len(assignedTopic.Partitions) > 0 {
					return true
				}
			}
		}
	}

	return false
}
