package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func parseEMF(t *testing.T, payload string) EMFMetricData {
	t.Helper()
	var data EMFMetricData
	require.NoError(t, json.Unmarshal([]byte(payload), &data))
	return data
}

func TestCreateEMFLogForIngressAccept(t *testing.T) {
	payload := CreateEMFLogForIngressAccept(
		"node-1",
		"policy-a",
		"ns-a",
		"10.0.0.1",
		"10.0.0.2",
		"pod-b",
		"TCP",
		80,
		8080,
	)

	emf := parseEMF(t, payload)
	require.Equal(t, "ingress_l4_connections_accepted", emf.Event.MetricName)
	require.Equal(t, "ingress", emf.Event.Direction)
	require.Equal(t, "ACCEPT", emf.Event.Verdict)
	require.Equal(t, "policy-a", emf.Event.PolicyName)
	require.Equal(t, "ns-a", emf.Event.PolicyNamespace)
	require.Equal(t, "pod-b", emf.Event.DstPodName)
	require.Equal(t, "10.0.0.1", emf.Event.SrcIP)
	require.Equal(t, 80, emf.Event.SrcPort)
	require.Equal(t, 8080, emf.Event.DstPort)
	require.Equal(t, "node-1", emf.Event.NodeName)

	require.Len(t, emf.AWS.CloudWatchMetrics, 1)
	spec := emf.AWS.CloudWatchMetrics[0]
	require.Equal(t, "AWSNetworkPolicy", spec.Namespace)
	require.Equal(t, []MetricDefinition{{Name: "ingress_l4_connections_accepted", Unit: "Count"}}, spec.Metrics)
	require.ElementsMatch(t, [][]string{
		{"MetricName"},
		{"MetricName", "PolicyNamespace"},
		{"MetricName", "PolicyNamespace", "PolicyName"},
	}, spec.Dimensions)
}

func TestCreateEMFLogForEgressAccept(t *testing.T) {
	payload := CreateEMFLogForEgressAccept(
		"node-1",
		"policy-a",
		"ns-a",
		"10.0.0.1",
		"10.0.0.2",
		"pod-a",
		"UDP",
		53,
		5353,
	)

	emf := parseEMF(t, payload)
	require.Equal(t, "egress_l4_connections_accepted", emf.Event.MetricName)
	require.Equal(t, "pod-a", emf.Event.SrcPodName)
	require.Equal(t, "policy-a", emf.Event.PolicyName)
	require.Equal(t, "ns-a", emf.Event.PolicyNamespace)
	require.Equal(t, "UDP", emf.Event.Protocol)
}

func TestCreateEMFLogForDenied(t *testing.T) {
	payload := CreateEMFLogForIngressDenied(
		"node-1",
		"10.0.0.1",
		"10.0.0.2",
		"pod-x",
		"TCP",
		443,
		8443,
	)

	emf := parseEMF(t, payload)
	require.Equal(t, "ingress_l4_connections_denied", emf.Event.MetricName)
	require.Equal(t, "DENY", emf.Event.Verdict)
	require.Empty(t, emf.Event.PolicyName)
	require.Empty(t, emf.Event.PolicyNamespace)
	require.Equal(t, "pod-x", emf.Event.DstPodName)
	require.ElementsMatch(t, [][]string{{"MetricName"}}, emf.AWS.CloudWatchMetrics[0].Dimensions)
}

func TestCreateEMFLogForDropped(t *testing.T) {
	payload := CreateEMFLogForEgressDropped(
		"node-1",
		"10.0.0.1",
		"10.0.0.2",
		"pod-y",
		"ANY",
		0,
		0,
	)

	emf := parseEMF(t, payload)
	require.Equal(t, "egress_l4_connections_dropped", emf.Event.MetricName)
	require.Equal(t, "pod-y", emf.Event.SrcPodName)
	require.Equal(t, "ANY", emf.Event.Protocol)
}

func TestCreateEMFLogHasTimestampAndMetric(t *testing.T) {
	payload := CreateEMFLogForIngressAccept(
		"node-1",
		"policy-a",
		"ns-a",
		"10.0.0.1",
		"10.0.0.2",
		"pod-b",
		"TCP",
		80,
		8080,
	)

	emf := parseEMF(t, payload)
	require.NotZero(t, emf.Timestamp)
	require.NotZero(t, emf.AWS.Timestamp)
	require.Equal(t, 1, emf.Metric[emf.Event.MetricName])

	delta := time.Since(time.UnixMilli(emf.Timestamp))
	require.Less(t, absDuration(delta), 2*time.Second)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
