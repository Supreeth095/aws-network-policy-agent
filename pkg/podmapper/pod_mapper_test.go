package podmapper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/amazon-vpc-cni-k8s/pkg/ipamd/datastore"
	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/require"
)

// fsnotifyTestWatcher exposes the ability to inject filesystem events into the watcher loop.
type fsnotifyTestWatcher interface {
	Add(name string) error
	Close() error
	Events() chan fsnotify.Event
	Errors() chan error
}

// newTestCheckpointData returns a checkpoint payload with the provided IPv4/metadata pairs.
func newTestCheckpointData(entries []datastore.CheckpointEntry) datastore.CheckpointData {
	return datastore.CheckpointData{
		Version:     "1",
		Allocations: entries,
	}
}

func writeCheckpoint(t *testing.T, path string, data datastore.CheckpointData) {
	t.Helper()
	bytes, err := json.Marshal(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, bytes, 0o600))
}

func TestLoadIPAMDataPopulatesMap(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "ipam.json")

	checkpoint := newTestCheckpointData([]datastore.CheckpointEntry{
		{
			IPv4: "10.0.0.5",
			Metadata: datastore.IPAMMetadata{
				K8SPodNamespace: "default",
				K8SPodName:      "pod-a",
			},
		},
		{
			IPv4: "10.0.0.6",
			IPv6: "2001:db8::1",
			Metadata: datastore.IPAMMetadata{
				K8SPodNamespace: "kube-system",
				K8SPodName:      "pod-b",
			},
		},
	})
	writeCheckpoint(t, file, checkpoint)

	mapper := &podMapper{ipamPath: file}
	require.NoError(t, mapper.loadIPAMData())

	require.Equal(t, "default/pod-a", mapper.GetPodName("10.0.0.5"))
	require.Equal(t, "kube-system/pod-b", mapper.GetPodName("10.0.0.6"))
	require.Equal(t, "kube-system/pod-b", mapper.GetPodName("2001:db8::1"))
	require.Equal(t, UnknownPod, mapper.GetPodName("10.0.0.7"))

	total, ipv4 := mapper.GetMappingStats()
	require.Equal(t, 3, total) // two IPv4 + one IPv6
	require.Equal(t, 2, ipv4)
}

func TestLoadIPAMDataSkipsEntriesWithoutMetadata(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "ipam.json")

	checkpoint := newTestCheckpointData([]datastore.CheckpointEntry{
		{
			IPv4: "10.0.0.5",
			Metadata: datastore.IPAMMetadata{
				K8SPodNamespace: "default",
				K8SPodName:      "pod-a",
			},
		},
		{
			IPv4:     "10.0.0.6",
			Metadata: datastore.IPAMMetadata{}, // missing metadata
		},
	})
	writeCheckpoint(t, file, checkpoint)

	mapper := &podMapper{ipamPath: file}
	require.NoError(t, mapper.loadIPAMData())

	require.Equal(t, "default/pod-a", mapper.GetPodName("10.0.0.5"))
	require.Equal(t, UnknownPod, mapper.GetPodName("10.0.0.6"))
}

func TestWatcherReloadsOnFileChange(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "ipam.json")

	initial := newTestCheckpointData([]datastore.CheckpointEntry{
		{
			IPv4: "10.0.0.5",
			Metadata: datastore.IPAMMetadata{
				K8SPodNamespace: "default",
				K8SPodName:      "pod-a",
			},
		},
	})
	writeCheckpoint(t, file, initial)

	mapper := &podMapper{ipamPath: file}

	// Replace fsnotify watcher with testable implementation
	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	mapper.watcher = watcher
	mapper.ctx, mapper.cancel = context.WithCancel(context.Background())

	require.NoError(t, mapper.loadIPAMData())

	mapper.wg.Add(1)
	go mapper.watcherLoop()
	defer func() {
		mapper.cancel()
		mapper.watcher.Close()
		mapper.wg.Wait()
	}()

	updated := newTestCheckpointData([]datastore.CheckpointEntry{
		{
			IPv4: "10.0.0.6",
			Metadata: datastore.IPAMMetadata{
				K8SPodNamespace: "ns",
				K8SPodName:      "pod-b",
			},
		},
	})
	writeCheckpoint(t, file, updated)

	select {
	case mapper.watcher.Events <- fsnotify.Event{Name: file, Op: fsnotify.Write}:
	case <-time.After(time.Second):
		t.Fatalf("timed out sending fsnotify event")
	}

	require.Eventually(t, func() bool {
		return mapper.GetPodName("10.0.0.6") == "ns/pod-b"
	}, time.Second, 20*time.Millisecond)
}

func TestGlobalPodMapperAccessors(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "ipam.json")
	checkpoint := newTestCheckpointData([]datastore.CheckpointEntry{
		{
			IPv4: "10.0.0.5",
			Metadata: datastore.IPAMMetadata{
				K8SPodNamespace: "default",
				K8SPodName:      "pod-a",
			},
		},
	})
	writeCheckpoint(t, file, checkpoint)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, InitializePodMapper(ctx, PodMapperConfig{IPAMPath: file}))
	defer ShutdownPodMapper()

	require.Equal(t, "default/pod-a", GetPodNameForIP("10.0.0.5"))

	total, ipv4 := GetGlobalMappingStats()
	require.Equal(t, 1, total)
	require.Equal(t, 1, ipv4)
}
