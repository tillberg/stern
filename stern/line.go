package stern

import (
	"os"
	"strings"
)

type Line struct {
	Prefix string
	Msg    string
}

func (l Line) Print() {
	os.Stderr.WriteString(l.Prefix)
	os.Stderr.WriteString(" ")
	// fmt.Fprintf(os.Stderr, "%d", len(l.Msg))
	os.Stderr.WriteString(l.Msg)
	os.Stderr.WriteString("\n")
}

// LineSlice sorts lines by timestamp (assuming a timestamp is the first element of msg before a space)
// and then by prefix. Use stable sort to maintain order within each container.
type LineSlice []Line

func (s LineSlice) Len() int      { return len(s) }
func (s LineSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s LineSlice) Less(i, j int) bool {
	tsi := s[i].Msg
	tsiSpace := strings.IndexByte(tsi, ' ')
	if tsiSpace >= 0 {
		tsi = tsi[:tsiSpace]
	}
	tsj := s[j].Msg
	tsjSpace := strings.IndexByte(tsj, ' ')
	if tsjSpace >= 0 {
		tsj = tsj[:tsjSpace]
	}
	cmp := strings.Compare(tsi, tsj)
	if cmp != 0 {
		return cmp < 0
	}
	return s[i].Prefix < s[j].Prefix
}
