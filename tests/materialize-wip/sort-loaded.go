package main

import (
	"bufio"
	"io"
	"os"
	"sort"
	"strings"
)

func main() {
	var r = bufio.NewReader(os.Stdin)
	var w = bufio.NewWriter(os.Stdout)

	var loads []string
	for {
		var line, err = r.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if strings.HasPrefix(line, "{\"loaded\":") {
			loads = append(loads, line)
		} else {
			sort.Strings(loads)
			for _, l := range loads {
				_, _ = w.WriteString(l)
			}
			loads = nil
			_, _ = w.WriteString(line)
		}
	}
	for _, l := range loads {
		_, _ = w.WriteString(l)
	}
	_ = w.Flush()
}
