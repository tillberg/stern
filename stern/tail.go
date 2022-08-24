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
	"bufio"
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
	"text/template"

	"github.com/fatih/color"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

var (
	rpcErrorNoSuchContainerBytes = "rpc error: code = Unknown desc = Error: No such container"
)

type Tail struct {
	Namespace      string
	PodName        string
	ContainerName  string
	Options        *TailOptions
	req            *rest.Request
	closed         chan struct{}
	podColor       *color.Color
	containerColor *color.Color
	tmpl           *template.Template
}

type TailOptions struct {
	Timestamps   bool
	SinceSeconds int64
	Exclude      []*regexp.Regexp
	Include      []*regexp.Regexp
	Namespace    bool
	TailLines    *int64
}

// NewTail returns a new tail for a Kubernetes container inside a pod
func NewTail(namespace, podName, containerName string, tmpl *template.Template, options *TailOptions) *Tail {
	return &Tail{
		Namespace:     namespace,
		PodName:       podName,
		ContainerName: containerName,
		Options:       options,
		closed:        make(chan struct{}),
		tmpl:          tmpl,
	}
}

var colorList = [][2]*color.Color{
	{color.New(color.FgHiCyan), color.New(color.FgCyan)},
	{color.New(color.FgHiGreen), color.New(color.FgGreen)},
	{color.New(color.FgHiMagenta), color.New(color.FgMagenta)},
	{color.New(color.FgHiYellow), color.New(color.FgYellow)},
	{color.New(color.FgHiBlue), color.New(color.FgBlue)},
	{color.New(color.FgHiRed), color.New(color.FgRed)},
}

func determineColor(podName string) (podColor, containerColor *color.Color) {
	hash := fnv.New32()
	hash.Write([]byte(podName))
	idx := hash.Sum32() % uint32(len(colorList))

	colors := colorList[idx]
	return colors[0], colors[1]
}

// Start starts tailing
func (t *Tail) Start(ctx context.Context, lines chan<- Line, i v1.PodInterface) {
	t.podColor, t.containerColor = determineColor(t.PodName)

	go func(ctx context.Context) {
		g := color.New(color.FgHiGreen, color.Bold).SprintFunc()
		p := t.podColor.SprintFunc()
		c := t.containerColor.SprintFunc()
		if t.Options.Namespace {
			fmt.Fprintf(os.Stderr, "%s %s %s › %s\n", g("+"), p(t.Namespace), p(t.PodName), c(t.ContainerName))
		} else {
			fmt.Fprintf(os.Stderr, "%s %s › %s\n", g("+"), p(t.PodName), c(t.ContainerName))
		}

		req := i.GetLogs(t.PodName, &corev1.PodLogOptions{
			Follow:       true,
			Timestamps:   t.Options.Timestamps,
			Container:    t.ContainerName,
			SinceSeconds: &t.Options.SinceSeconds,
			TailLines:    t.Options.TailLines,
		})

		stream, err := req.Stream(ctx)
		if err != nil {
			fmt.Println(errors.Wrapf(err, "Error opening stream to %s/%s: %s\n", t.Namespace, t.PodName, t.ContainerName))
			return
		}
		defer stream.Close()

		go func() {
			<-t.closed
			stream.Close()
		}()

		var containerPrefix string
		containerPrefix = t.containerColor.Sprint(fmt.Sprintf("% 10s", t.ContainerName))

		scanner := bufio.NewScanner(stream)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, rpcErrorNoSuchContainerBytes) {
				continue
			}

			str := string(line)

			exclude := false
			for _, rex := range t.Options.Exclude {
				if rex.MatchString(str) {
					exclude = true
					break
				}
			}
			if exclude {
				continue
			}

			if len(t.Options.Include) != 0 {
				matches := false
				for _, rin := range t.Options.Include {
					if rin.MatchString(str) {
						matches = true
						break
					}
				}
				if !matches {
					continue
				}
			}

			lines <- Line{Prefix: containerPrefix, Msg: str}
		}
		err = scanner.Err()
		if err != nil {
			errStr := err.Error()
			switch {
			case strings.Contains(errStr, "response body closed"):
			default:
				fmt.Println(errors.Wrapf(err, "Error reading stream for %s/%s: %s\n", t.Namespace, t.PodName, t.ContainerName))
			}
			return
		}
	}(ctx)

	go func() {
		<-ctx.Done()
		close(t.closed)
	}()
}

// Close stops tailing
func (t *Tail) Close() {
	r := color.New(color.FgHiRed, color.Bold).SprintFunc()
	p := t.podColor.SprintFunc()
	if t.Options.Namespace {
		fmt.Fprintf(os.Stderr, "%s %s %s\n", r("-"), p(t.Namespace), p(t.PodName))
	} else {
		fmt.Fprintf(os.Stderr, "%s %s\n", r("-"), p(t.PodName))
	}
	close(t.closed)
}
