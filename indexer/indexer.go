package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/messiaen/es-indexer/messages"
	"github.com/olivere/elastic"
)

const docType = "_doc"

type EsIndexerConfig struct {
	InitRetryDur      time.Duration
	MaxRetryDur       time.Duration
	EsUrls            []string
	BulkWorkers       int
	BulkActions       int
	BulkBytes         int
	BulkFlushInterval time.Duration
	BulkBeforeFunc    EsIndexerBeforeFunc
	BulkAfterFunc     EsIndexerAfterFunc
}

func DefaultEsIndexerConfig() EsIndexerConfig {
	return EsIndexerConfig{
		InitRetryDur:      1 * time.Second,
		MaxRetryDur:       1 * time.Second,
		EsUrls:            []string{"http://localhost:9200"},
		BulkWorkers:       1,
		BulkActions:       100,
		BulkBytes:         1000000,
		BulkFlushInterval: 1 * time.Second,
	}
}

type EsIndexer struct {
	es       *elastic.Client
	ctx      context.Context
	bulkCtx  context.Context
	c        EsIndexerConfig
	bulkProc *elastic.BulkProcessor
}

type EsIndexerTraceInfo interface {
	GetID() string
	GetContentID() string
	GetSource() string
}

type EsIndexerBulkableRequest interface {
	EsIndexerTraceInfo
	elastic.BulkableRequest
}

type esIndexerTraceInfo struct {
	id        string
	contentID string
	source    string
}

func (ti *esIndexerTraceInfo) GetID() string {
	return ti.id
}

func (ti *esIndexerTraceInfo) GetContentID() string {
	return ti.contentID
}

func (ti *esIndexerTraceInfo) GetSource() string {
	return ti.source
}

type esIndexerBulkableRequest struct {
	elastic.BulkableRequest
	EsIndexerTraceInfo
}

var _ EsIndexerBulkableRequest = &esIndexerBulkableRequest{}

type EsIndexerAfterFunc func(executionId int64, requests []EsIndexerBulkableRequest, response *elastic.BulkResponse, err error)
type EsIndexerBeforeFunc func(executionId int64, requests []EsIndexerBulkableRequest)

func NewEsIndexer(config EsIndexerConfig) (*EsIndexer, error) {
	esi := EsIndexer{
		c:       config,
		ctx:     context.Background(),
		bulkCtx: context.Background(),
	}

	client, err := elastic.NewClient(
		elastic.SetURL(config.EsUrls...),
		elastic.SetRetrier(
			elastic.NewBackoffRetrier(
				elastic.NewExponentialBackoff(config.InitRetryDur, config.MaxRetryDur))),
	)
	if err != nil {
		return nil, fmt.Errorf("Error creating ES client -- %v", err)
	}
	esi.es = client

	bulkProc, err := elastic.NewBulkProcessorService(esi.es).
		BulkActions(config.BulkActions).
		BulkSize(config.BulkBytes).
		FlushInterval(config.BulkFlushInterval).
		Workers(config.BulkWorkers).
		After(esi.wrapAfter).
		Before(esi.wrapBefore).
		Do(esi.bulkCtx)
	if err != nil {
		return nil, fmt.Errorf("Error creating bulk processor -- %v", err)
	}
	esi.bulkProc = bulkProc

	return &esi, nil
}

func (esi *EsIndexer) wrapAfter(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		fmt.Printf("Index Error: %v", err)
	}
	if esi.c.BulkAfterFunc != nil {
		reqs := toEsIndexerBulkableRequests(requests)
		esi.c.BulkAfterFunc(executionId, reqs, response, err)
	}
}

func (esi *EsIndexer) wrapBefore(executionId int64, requests []elastic.BulkableRequest) {
	if esi.c.BulkBeforeFunc != nil {
		reqs := toEsIndexerBulkableRequests(requests)
		esi.c.BulkBeforeFunc(executionId, reqs)
	}
}

func (esi *EsIndexer) Store(storeReq *messages.IndexerRequest) error {
	bulkReqs, err := createBulkableRequests(storeReq)
	if err != nil {
		return fmt.Errorf("Failed to store request -- %v", err)
	}
	for _, r := range bulkReqs {
		esi.bulkProc.Add(r)
	}
	return nil
}

func (esi *EsIndexer) Close() {
	esi.bulkProc.Flush()
	esi.bulkProc.Close()
	esi.es.Stop()
}

func toEsIndexerBulkableRequests(requests []elastic.BulkableRequest) []EsIndexerBulkableRequest {
	reqs := make([]EsIndexerBulkableRequest, len(requests))
	for i, r := range requests {
		_r, ok := r.(EsIndexerBulkableRequest)
		if !ok {
			// this should never happen so if it does panic
			panic("Invalid bulkable request")
		}
		reqs[i] = _r
	}
	return reqs
}

func createBulkableRequests(req *messages.IndexerRequest) ([]EsIndexerBulkableRequest, error) {
	info := &esIndexerTraceInfo{
		id:        req.GetTraceId(),
		contentID: req.GetContentId(),
		source:    req.GetSource(),
	}

	bulkableReqs := make([]EsIndexerBulkableRequest, len(req.GetDocuments()))
	for i, reqDoc := range req.GetDocuments() {
		op := reqDoc.GetOperation()
		var req EsIndexerBulkableRequest
		var err error
		switch op {
		case messages.OpIndex:
			req, err = toIndexRequest(info, reqDoc)
		case messages.OpUpdate:
			req, err = toUpdateRequest(info, reqDoc)
		default:
			return nil, fmt.Errorf("Failed to create bulkrequest for document %v.  Invalid operation %v", reqDoc, op)
		}
		if err != nil {
			return nil, fmt.Errorf("Failed to create bulk request for document %v -- %v", reqDoc, err)
		}
		bulkableReqs[i] = req
	}
	return bulkableReqs, nil
}

func toIndexRequest(info EsIndexerTraceInfo, doc *messages.IndexerRequestDocument) (EsIndexerBulkableRequest, error) {
	docInt, err := toDocInterface(doc.GetDocument())
	if err != nil {
		return nil, err
	}
	elasticReq := elastic.NewBulkIndexRequest().
		Index(doc.GetIndex()).
		Type(docType).
		Doc(docInt)

	if doc.GetID() != "" {
		elasticReq = elasticReq.Id(doc.GetID())
	}

	return &esIndexerBulkableRequest{
		BulkableRequest:    elasticReq,
		EsIndexerTraceInfo: info,
	}, nil
}

func toUpdateRequest(info EsIndexerTraceInfo, doc *messages.IndexerRequestDocument) (*esIndexerBulkableRequest, error) {
	if doc.GetID() == "" {
		return nil, fmt.Errorf("ID must be specified to update document")
	}
	docInt, err := toDocInterface(doc.GetDocument())
	if err != nil {
		return nil, err
	}
	elasticReq := elastic.NewBulkUpdateRequest().
		DocAsUpsert(true).
		DetectNoop(true).
		Index(doc.GetIndex()).
		Type(docType).
		Id(doc.GetID()).
		Doc(docInt)

	return &esIndexerBulkableRequest{
		BulkableRequest:    elasticReq,
		EsIndexerTraceInfo: info,
	}, nil
}

func toDocInterface(docStr string) (map[string]interface{}, error) {
	doc := map[string]interface{}{}
	err := json.Unmarshal([]byte(docStr), &doc)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse document %v -- %v", docStr, err)
	}
	return doc, nil
}
