//   Copyright 2016 Wercker Holding BV
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package stern

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/tillberg/stern/kubernetes"
)

// Run starts the main run loop
func Run(ctx context.Context, config *Config) error {
	clientConfig := kubernetes.NewClientConfig(config.KubeConfig, config.ContextName)
	clientset, err := kubernetes.NewClientSet(clientConfig)
	if err != nil {
		return err
	}

	var namespace string
	// A specific namespace is ignored if all-namespaces is provided
	if config.AllNamespaces {
		namespace = ""
	} else {
		namespace = config.Namespace
		if namespace == "" {
			namespace, _, err = clientConfig.Namespace()
			if err != nil {
				return errors.Wrap(err, "unable to get default namespace")
			}
		}
	}

	added, removed, err := Watch(ctx, clientset.CoreV1().Pods(namespace), config.PodQuery, config.ContainerQuery, config.ExcludeContainerQuery, config.ContainerState, config.LabelSelector)
	if err != nil {
		return errors.Wrap(err, "failed to set up watch")
	}

	tails := make(map[string]*Tail)
	lines := make(chan Line, 100)

	go func() {
		startupDelayTime := time.Now().Add(1 * time.Second)
		startupDelayTimer := time.After(1 * time.Second)
		var startupLines LineSlice
	startupLoop:
		for {
			select {
			case line := <-lines:
				startupLines = append(startupLines, line)
				if time.Now().Add(200 * time.Millisecond).After(startupDelayTime) {
					startupDelayTime = time.Now().Add(250 * time.Millisecond)
					startupDelayTimer = time.After(250 * time.Millisecond)
				}
			case <-startupDelayTimer:
				break startupLoop
			}
		}
		sort.Stable(startupLines)
		for _, line := range startupLines {
			line.Print()
		}
		startupLines = nil
		for line := range lines {
			line.Print()
		}
	}()

	go func() {
		for p := range added {
			id := p.GetID()
			if tails[id] != nil {
				// alog.Printf("Added container %q already exists, ignoring\n", id)
				continue
			}

			tail := NewTail(p.Namespace, p.Pod, p.Container, config.Template, &TailOptions{
				Timestamps:   config.Timestamps,
				SinceSeconds: int64(config.Since.Seconds()),
				Exclude:      config.Exclude,
				Include:      config.Include,
				Namespace:    config.AllNamespaces,
				TailLines:    config.TailLines,
			})
			tails[id] = tail

			tail.Start(ctx, lines, clientset.CoreV1().Pods(p.Namespace))
		}
	}()

	go func() {
		for p := range removed {
			id := p.GetID()
			if tails[id] == nil {
				// alog.Printf("Removed container %q does not exist, ignoring\n", id)
				continue
			}
			tails[id].Close()
			delete(tails, id)
		}
	}()

	<-ctx.Done()

	return nil
}
