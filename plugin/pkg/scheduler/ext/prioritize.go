package ext

import (
	"fmt"
	"strings"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/kubernetes/plugin/pkg/scheduler/schedulercache"
	"k8s.io/kubernetes/plugin/pkg/scheduler/util"

	"github.com/golang/glog"
)

type Prioritizer interface {
	Prioritize(pod1, pod2 interface{}) bool
}

type GPUUsagePriortizer struct {
	podCache schedulercache.Cache
}

func NewGPUUsagePriortizer(cache schedulercache.Cache) Prioritizer {
	return &GPUUsagePriortizer{
		podCache: cache,
	}
}

func (p *GPUUsagePriortizer) Prioritize(pod1, pod2 interface{}) bool {
	p1 := pod1.(*v1.Pod)
	p2 := pod2.(*v1.Pod)

	isJob1, user1, gpu1 := isRisemlGPUJob(p1)
	isJob2, user2, gpu2 := isRisemlGPUJob(p2)

	fmt.Printf("%v %v %v\n", isJob1, user1, gpu1)
	fmt.Printf("%v %v %v\n", isJob2, user2, gpu2)

	if !isJob1 && !isJob2 {
		return util.ElderPod(pod1, pod2)
	}

	if isJob1 && isJob2 {
		user1Usage := p.getUserGPUUsage(user1) + gpu1
		user2Usage := p.getUserGPUUsage(user2) + gpu2
		fmt.Printf("%v %v\n", user1Usage, user2Usage)
		if user1Usage == user2Usage {
			return util.ElderPod(pod1, pod2)
		}

		return user1Usage < user2Usage
	}

	// isJob1 == false isJob2 == true
	// isJob1 == true isJob2 == false
	return !isJob1
}

const RisemlUserKey = "username"

func (p *GPUUsagePriortizer) getUserGPUUsage(user string) int64 {
	selector := labels.NewSelector()
	req, err := labels.NewRequirement(RisemlUserKey, selection.Equals, []string{user})
	if err != nil {
		glog.Warning("Fail to create requirement:", err.Error())
		return 0
	}

	selector = selector.Add(*req)
	pods, err := p.podCache.List(selector)
	if err != nil {
		glog.Warning("Fail to list pods of user ", user, err.Error())
		return 0
	}

	var total int64
	for _, pod := range pods {
		isJob, _, gpu := isRisemlGPUJob(pod)
		if !isJob {
			continue
		}

		// count only scheduled pod
		if pod.Spec.NodeName != "" {
			total += gpu
		}
	}

	return total
}

func isRisemlGPUJob(pod *v1.Pod) (bool, string, int64) {
	username := getUserName(pod.Name)
	gpu := getGPURequests(pod)

	return username != "" && gpu != 0, username, gpu
}

// extract the owner name from the riseml job pod name
// podName: {username}-{jobname}-{jobid}-train
func getUserName(podName string) string {
	if !strings.HasSuffix(podName, "-train") {
		// not a riseml job name
		return ""
	}

	tokens := strings.Split(podName, "-")
	if len(tokens) != 4 {
		// not a riseml job name
		return ""
	}

	return tokens[0]
}

func getGPURequests(pod *v1.Pod) int64 {
	var gpu int64
	for _, container := range pod.Spec.Containers {
		gpu += container.Resources.Requests.NvidiaGPU().Value()
	}

	return gpu
}
