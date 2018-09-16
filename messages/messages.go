package messages

type IndexerRequestOp int

const (
	OpIndex  IndexerRequestOp = 1
	OpUpdate IndexerRequestOp = 2
)

type IndexerRequestDocument struct {
	ID        string
	Document  string
	Index     string
	Operation IndexerRequestOp
}

func (ird *IndexerRequestDocument) GetID() string {
	return ird.ID
}

func (ird *IndexerRequestDocument) GetDocument() string {
	return ird.Document
}

func (ird *IndexerRequestDocument) GetIndex() string {
	return ird.Index
}

func (ird *IndexerRequestDocument) GetOperation() IndexerRequestOp {
	return ird.Operation
}

type IndexerRequest struct {
	TraceId   string
	ContentId string
	Source    string
	// slice of pointer for consistency with thrift generated code
	Documents []*IndexerRequestDocument
}

func (ir *IndexerRequest) GetTraceId() string {
	return ir.TraceId
}

func (ir *IndexerRequest) GetContentId() string {
	return ir.ContentId
}

func (ir *IndexerRequest) GetSource() string {
	return ir.Source
}

func (ir *IndexerRequest) GetDocuments() []*IndexerRequestDocument {
	return ir.Documents
}
