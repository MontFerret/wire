package outputstream

import "github.com/MontFerret/api"

// Accumulator materializes only bytes actually delivered. Its first Add marks
// content present, including for an empty chunk; metadata never sizes storage.
type Accumulator struct {
	Metadata api.Metadata
	content  *api.Content
}

// Add copies borrowed bytes before the next receive can reuse them.
func (a *Accumulator) Add(data []byte) {
	if a.content == nil {
		a.content = &api.Content{Metadata: a.Metadata}
	}

	a.content.Data = append(a.content.Data, data...)
}

// Content transfers the accumulated detached content to the caller.
func (a *Accumulator) Content() *api.Content { return a.content }
