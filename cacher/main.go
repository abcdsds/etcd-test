package main

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	_ "k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	_ "k8s.io/apimachinery/pkg/runtime/serializer/versioning"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	_ "k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	"log"
	"time"
)

var (
	scheme   = runtime.NewScheme()
	codecs   = serializer.NewCodecFactory(scheme)
	errDummy = fmt.Errorf("dummy error")
)

func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
}

func main() {
	etcdConfig := &storagebackend.Config{
		Type: storagebackend.StorageTypeETCD3,
		Transport: storagebackend.TransportConfig{
			TrustedCAFile: "ca.crt",
			CertFile:      "server.crt",
			KeyFile:       "server.key",
			ServerList:    []string{":2379"},
		},
		Codec: apitesting.TestStorageCodec(codecs, examplev1.SchemeGroupVersion),
	}

	// ConfigResource를 생성합니다.
	configResource := storagebackend.ConfigForResource{
		Config: *etcdConfig,
		GroupResource: schema.GroupResource{
			Resource: "pods",
		},
	}

	podPrefix := "/pods"
	newFunc := func() runtime.Object { return &example.Pod{} }

	s, _, err := factory.Create(configResource, newFunc)
	if err != nil {
		fmt.Errorf("error creating storage: %v", err)
	}

	cfg := cacher.Config{
		Storage:        s,
		Versioner:      s.Versioner(),
		ResourcePrefix: podPrefix,
		GroupResource:  configResource.GroupResource,
		KeyFunc:        func(obj runtime.Object) (string, error) { return storage.NoNamespaceKeyFunc(podPrefix, obj) },
		GetAttrsFunc:   getPodAttrs,
		NewFunc:        newFunc,
		NewListFunc:    func() runtime.Object { return &example.PodList{} },
		Codec:          apitesting.TestStorageCodec(codecs, examplev1.SchemeGroupVersion),
		//Codec: codecs.LegacyCodec(examplev1.SchemeGroupVersion),
		Clock: clock.RealClock{},
	}

	// NewCacherFromConfig 함수를 사용해 Cacher 생성
	c, err := cacher.NewCacherFromConfig(cfg)
	if err != nil {
		log.Fatal("err config ", err)
	}

	defer c.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := &example.Pod{}
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foo-%d", 0), Namespace: "dscho", Labels: map[string]string{
		"aa": "bb",
	}}}
	key := computePodKey(obj)

	//if err := c.Create(ctx, key, obj, out, 0); err != nil {
	//	log.Fatal("create ", err)
	//}

	//if err := c.Delete(ctx, key, out, nil, storage.ValidateAllObjectFunc, nil); err != nil {
	//	log.Fatal("delete ", err)
	//}

	options := storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Limit: 500,
			Label: labels.Everything(),
			Field: fields.Everything(),
		},
	}

	for i := 0; i < 100; i++ {
		go func() {
			watch, err := c.Watch(ctx, key, options)
			if err != nil {
				log.Fatal("watch ", err)
			}
			fmt.Println("created watch")

			ch := watch.ResultChan()
			for event := range ch {
				fmt.Printf("Event type: %s, object: %v\n", event.Type, event.Object)
			}
		}()
	}

	time.Sleep(2 * time.Second)

	fmt.Println("start update")

	updateFn := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		obj.Labels = map[string]string{"cc": "dd99"}
		return obj.DeepCopyObject(), nil, nil
	}
	if err := c.GuaranteedUpdate(ctx, key, out, true, nil, updateFn, nil); err != nil {
		log.Fatal("update ", err)
	}

	fmt.Println("end")
	time.Sleep(1 * time.Hour)
}

func getPodAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod := obj.(*example.Pod)
	return labels.Set{"name": pod.ObjectMeta.Name}, nil, nil
}

func computePodKey(obj *example.Pod) string {
	return fmt.Sprintf("/pods/%s/%s", obj.Namespace, obj.Name)
}
