package ext

import (
	"testing"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
)

func TestPrioritize(t *testing.T) {
	cache := makeCache()
	container0 := makeContainer("")
	container1 := makeContainer("1")
	container2 := makeContainer("2")
	p := NewGPUUsagePriortizer(cache)
	tests := []struct {
		Pod1   *v1.Pod
		Pod2   *v1.Pod
		Expect bool
	}{
		{
			Pod1:   makePod("user1-test-1-train", "", container0, container1),
			Pod2:   makePod("user3-test-1-train", "", container0, container1),
			Expect: true,
		},
		{
			Pod1:   makePod("user3-test-1-train", "", container0, container1),
			Pod2:   makePod("user5-test-0-train", "", container0),
			Expect: false,
		},
		{
			Pod1:   makePod("user1-test-4-train", "", container2, container2),
			Pod2:   makePod("user5-test-1-train", "", container0, container1),
			Expect: true,
		},
		{
			Pod1:   makePod("user1-test-6-train", "", container2, container2, container2),
			Pod2:   makePod("user5-test-1-train", "", container0, container1),
			Expect: false,
		},
		{
			Pod1:   makePod("notjob", "", container2),
			Pod2:   makePod("notjob2", "", container1),
			Expect: true,
		},
		{
			Pod1:   makePod("user1-test-1-train", "", container2),
			Pod2:   makePod("notjob", "", container2),
			Expect: false,
		},
		{
			Pod1:   makePod("user1-test-0-train", "", container0),
			Pod2:   makePod("notjob", "", container2),
			Expect: true,
		},
	}

	for _, testCase := range tests {
		actual := p.Prioritize(testCase.Pod1, testCase.Pod2)
		if actual == testCase.Expect {
			t.Fatalf("Pod1: %s, Pod2: %s, Expect: %t, Actual: %t",
				testCase.Pod1.Name, testCase.Pod2.Name, testCase.Expect, actual)
		}
	}
}

func makeContainer(gpu string) v1.Container {
	req := v1.ResourceList{}
	if gpu != "" {
		req = v1.ResourceList{
			v1.ResourceNvidiaGPU: resource.MustParse(gpu),
		}
	}

	return v1.Container{
		Resources: v1.ResourceRequirements{
			Requests: req,
		},
	}
}

func makePod(podName, nodeName string, containers ...v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "test_ext_prioritize_ns",
			Name:              podName,
			CreationTimestamp: metav1.Time{Time: time.Now()},
			Labels: map[string]string{
				"username": getUserName(podName),
			},
		},
		Spec: v1.PodSpec{
			Containers: containers,
			NodeName:   nodeName,
		},
	}
}

func makeCache() schedulercache.Cache {
	cache := schedulercache.New(time.Hour, make(chan struct{}))
	container0 := makeContainer("")
	container1 := makeContainer("1")
	container2 := makeContainer("2")

	// job
	pod := makePod("user1-abc-0-train", "nodea", container0)
	cache.AddPod(pod)
	pod = makePod("user1-abc-1-train", "nodeb", container1, container0)
	cache.AddPod(pod)
	pod = makePod("user3-abc-1-train", "nodec", container1)
	cache.AddPod(pod)
	pod = makePod("user3-abc-2-train", "nodea", container1, container1)
	cache.AddPod(pod)
	pod = makePod("user5-abc-2-train", "nodeb", container1, container0, container0)
	cache.AddPod(pod)
	pod = makePod("user5-abc-4-train", "nodec", container2, container1, container0, container1)
	cache.AddPod(pod)

	// non job
	pod = makePod("pod", "nodea", container1)
	cache.AddPod(pod)

	// pending job
	pod = makePod("pod-pending", "", container2)
	cache.AddPod(pod)

	return cache
}
