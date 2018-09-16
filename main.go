package main

import (
	"github.com/messiaen/es-indexer/indexer"
	"github.com/messiaen/es-indexer/messages"
)

func main() {
	config := indexer.DefaultEsIndexerConfig()
	es, err := indexer.NewEsIndexer(config)
	if err != nil {
		panic(err)
	}
	defer es.Close()

	sampleReq := createSampleReq()
	es.Store(&sampleReq)
}

const testDoc = `{
	"contentId": "content-1234",
	"tags": ["English", "test", "elastic"],
	"description": "This is a test document. The Quick brown Fox"
}
`

func createSampleReq() messages.IndexerRequest {
	return messages.IndexerRequest{
		TraceId:   "trace-1234",
		ContentId: "content-1234",
		Source:    "self",
		Documents: []*messages.IndexerRequestDocument{
			{
				Operation: messages.OpIndex,
				ID:        "",
				Index:     "my-first-index",
				Document:  testDoc,
			},
		},
	}
}
