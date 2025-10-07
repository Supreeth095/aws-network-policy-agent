package events

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-network-policy-agent/pkg/aws/services"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/require"
)

// fakeCloudWatchLogs implements services.CloudWatchLogs for testing purposes.
type fakeCloudWatchLogs struct {
	mu           sync.Mutex
	inputs       []*cloudwatchlogs.PutLogEventsInput
	nextTokens   []string
	err          error
	blockCh      chan struct{}
	blockStarted chan struct{}
}

var _ services.CloudWatchLogs = (*fakeCloudWatchLogs)(nil)

func newFakeCloudWatchLogs() *fakeCloudWatchLogs {
	return &fakeCloudWatchLogs{}
}

func (f *fakeCloudWatchLogs) record(input *cloudwatchlogs.PutLogEventsInput) {
	copyInput := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  input.LogGroupName,
		LogStreamName: input.LogStreamName,
	}
	if input.SequenceToken != nil {
		token := *input.SequenceToken
		copyInput.SequenceToken = &token
	}
	copiedEvents := make([]types.InputLogEvent, len(input.LogEvents))
	for i, ev := range input.LogEvents {
		copiedEvents[i] = types.InputLogEvent{}
		if ev.Message != nil {
			msg := *ev.Message
			copiedEvents[i].Message = aws.String(msg)
		}
		if ev.Timestamp != nil {
			ts := *ev.Timestamp
			copiedEvents[i].Timestamp = aws.Int64(ts)
		}
	}
	copyInput.LogEvents = copiedEvents
	f.inputs = append(f.inputs, copyInput)
}

func (f *fakeCloudWatchLogs) PutLogEvents(ctx context.Context, input *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	if f.blockCh != nil {
		if f.blockStarted != nil {
			select {
			case f.blockStarted <- struct{}{}:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.blockCh:
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	f.record(input)

	output := &cloudwatchlogs.PutLogEventsOutput{}
	if len(f.nextTokens) > 0 {
		next := f.nextTokens[0]
		f.nextTokens = f.nextTokens[1:]
		if next != "" {
			output.NextSequenceToken = aws.String(next)
		}
	}

	return output, nil
}

func (f *fakeCloudWatchLogs) CreateLogGroup(context.Context, *cloudwatchlogs.CreateLogGroupInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error) {
	return &cloudwatchlogs.CreateLogGroupOutput{}, nil
}

func (f *fakeCloudWatchLogs) CreateLogStream(context.Context, *cloudwatchlogs.CreateLogStreamInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	return &cloudwatchlogs.CreateLogStreamOutput{}, nil
}

func (f *fakeCloudWatchLogs) DescribeLogGroups(context.Context, *cloudwatchlogs.DescribeLogGroupsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	return &cloudwatchlogs.DescribeLogGroupsOutput{}, nil
}

func (f *fakeCloudWatchLogs) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inputs)
}

func (f *fakeCloudWatchLogs) eventsForCall(idx int) []types.InputLogEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.inputs) {
		return nil
	}
	return f.inputs[idx].LogEvents
}

func (f *fakeCloudWatchLogs) sequenceTokenForCall(idx int) *string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.inputs) {
		return nil
	}
	return f.inputs[idx].SequenceToken
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func totalEvents(fake *fakeCloudWatchLogs) int {
	total := 0
	for i := 0; i < fake.callCount(); i++ {
		total += len(fake.eventsForCall(i))
	}
	return total
}

func TestAsyncCloudWatchUploader_FlushOnBatchSize(t *testing.T) {
	fake := newFakeCloudWatchLogs()
	uploader := NewAsyncCloudWatchUploader(AsyncCloudWatchConfig{
		BatchSize:     3,
		BatchTimeout:  5 * time.Second,
		ChannelBuffer: 10,
		LogGroupName:  "test-group",
		LogStreamName: "test-stream",
		CWLClient:     fake,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, uploader.Start(ctx))
	defer uploader.Stop()

	for i := 0; i < 3; i++ {
		uploader.SendEvent("event")
	}

	waitForCondition(t, 500*time.Millisecond, func() bool { return fake.callCount() == 1 })
	require.Len(t, fake.eventsForCall(0), 3)
}

func TestAsyncCloudWatchUploader_FlushOnTimeout(t *testing.T) {
	fake := newFakeCloudWatchLogs()
	uploader := NewAsyncCloudWatchUploader(AsyncCloudWatchConfig{
		BatchSize:     5,
		BatchTimeout:  20 * time.Millisecond,
		ChannelBuffer: 5,
		LogGroupName:  "test-group",
		LogStreamName: "test-stream",
		CWLClient:     fake,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, uploader.Start(ctx))
	defer uploader.Stop()

	uploader.SendEvent("timeout-event")

	waitForCondition(t, 500*time.Millisecond, func() bool { return fake.callCount() == 1 })
	require.Len(t, fake.eventsForCall(0), 1)
}

func TestAsyncCloudWatchUploader_FlushOnStop(t *testing.T) {
	fake := newFakeCloudWatchLogs()
	uploader := NewAsyncCloudWatchUploader(AsyncCloudWatchConfig{
		BatchSize:     5,
		BatchTimeout:  time.Second,
		ChannelBuffer: 5,
		LogGroupName:  "test-group",
		LogStreamName: "test-stream",
		CWLClient:     fake,
	})

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, uploader.Start(ctx))

	uploader.SendEvent("shutdown-1")
	uploader.SendEvent("shutdown-2")

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, fake.callCount())

	uploader.Stop()
	cancel()

	require.Equal(t, 1, fake.callCount())
	require.Equal(t, 2, totalEvents(fake))
}

func TestAsyncCloudWatchUploader_ChannelOverflow(t *testing.T) {
	fake := newFakeCloudWatchLogs()
	fake.blockCh = make(chan struct{})
	fake.blockStarted = make(chan struct{}, 1)

	uploader := NewAsyncCloudWatchUploader(AsyncCloudWatchConfig{
		BatchSize:     1,
		BatchTimeout:  time.Second,
		ChannelBuffer: 1,
		LogGroupName:  "test-group",
		LogStreamName: "test-stream",
		CWLClient:     fake,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, uploader.Start(ctx))
	defer uploader.Stop()

	uploader.SendEvent("blocked")
	<-fake.blockStarted

	uploader.SendEvent("queued")
	uploader.SendEvent("dropped")

	close(fake.blockCh)

	waitForCondition(t, 500*time.Millisecond, func() bool { return fake.callCount() == 2 })
	require.Equal(t, 2, totalEvents(fake))
}

func TestAsyncCloudWatchUploader_SequenceTokenPropagation(t *testing.T) {
	fake := newFakeCloudWatchLogs()
	fake.nextTokens = []string{"token-1", "token-2"}

	uploader := NewAsyncCloudWatchUploader(AsyncCloudWatchConfig{
		BatchSize:     1,
		BatchTimeout:  time.Second,
		ChannelBuffer: 2,
		LogGroupName:  "test-group",
		LogStreamName: "test-stream",
		CWLClient:     fake,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, uploader.Start(ctx))
	defer uploader.Stop()

	uploader.SendEvent("event-1")
	waitForCondition(t, 500*time.Millisecond, func() bool { return fake.callCount() >= 1 })

	uploader.SendEvent("event-2")
	waitForCondition(t, 500*time.Millisecond, func() bool { return fake.callCount() >= 2 })

	require.Nil(t, fake.sequenceTokenForCall(0))
	require.Equal(t, "token-1", aws.ToString(fake.sequenceTokenForCall(1)))
}
