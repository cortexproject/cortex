package distributed_execution

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/promql-engine/execution/exchange"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/cortexproject/cortex/pkg/distributed_execution/querierpb"
	"github.com/cortexproject/cortex/pkg/ring/client"
)

const (
	RemoteNode = "RemoteNode"
)

// (to verify interface implementations)
var _ logicalplan.Node = (*Remote)(nil)

// Remote is a custom node that marks where the portion of logical plan
// that needs to be executed remotely
type Remote struct {
	Expr logicalplan.Node `json:"-"`

	FragmentKey  FragmentKey
	FragmentAddr string

	clientPool *client.Pool
}

func NewRemoteNode(Expr logicalplan.Node) logicalplan.Node {
	return &Remote{
		// initialize the fragment key pointer first
		Expr:        Expr,
		FragmentKey: FragmentKey{},
	}
}
func (r *Remote) Clone() logicalplan.Node {
	return &Remote{Expr: r.Expr.Clone(), FragmentKey: r.FragmentKey, FragmentAddr: r.FragmentAddr}
}
func (r *Remote) Children() []*logicalplan.Node {
	return []*logicalplan.Node{&r.Expr}
}
func (r *Remote) String() string {
	return fmt.Sprintf("remote(%s)", r.Expr.String())
}
func (r *Remote) ReturnType() parser.ValueType {
	return r.Expr.ReturnType()
}
func (r *Remote) Type() logicalplan.NodeType { return RemoteNode }

func (r *Remote) InsertClientPool(clientPool *client.Pool) {
	r.clientPool = clientPool
}

type remote struct {
	QueryID      uint64
	FragmentID   uint64
	FragmentAddr string
}

func (r *Remote) MarshalJSON() ([]byte, error) {
	return json.Marshal(remote{
		QueryID:      r.FragmentKey.queryID,
		FragmentID:   r.FragmentKey.fragmentID,
		FragmentAddr: r.FragmentAddr,
	})
}

func (r *Remote) UnmarshalJSON(data []byte) error {
	re := remote{}
	if err := json.Unmarshal(data, &re); err != nil {
		return err
	}

	r.FragmentKey = MakeFragmentKey(re.QueryID, re.FragmentID)
	r.FragmentAddr = re.FragmentAddr
	return nil
}

// MakeExecutionOperator creates a distributed execution operator from a Remote node.
// This implements the logicalplan.UserDefinedExpr interface, allowing Remote nodes
// to be transformed into custom distributed execution operators during query processing.
func (r *Remote) MakeExecutionOperator(
	ctx context.Context,
	vectors *model.VectorPool,
	opts *query.Options,
	hints storage.SelectHints,
) (model.VectorOperator, error) {
	pool := r.clientPool

	remoteExec, err := newDistributedRemoteExecution(ctx, pool, r.FragmentKey, opts)
	if err != nil {
		return nil, err
	}
	remoteExec.vectors = vectors

	return exchange.NewConcurrent(remoteExec, 2, opts), nil
}

type DistributedRemoteExecution struct {
	client querierpb.QuerierClient

	vectors *model.VectorPool

	mint        int64
	maxt        int64
	step        int64
	currentStep int64
	numSteps    int

	stream      querierpb.Querier_NextClient
	buffer      []model.StepVector
	bufferIndex int

	batchSize   int64
	series      []labels.Labels
	fragmentKey FragmentKey
	addr        string
	initialized bool // track if stream is initialized
}

type QuerierAddrKey struct{}

// newDistributedRemoteExecution creates a DistributedRemoteExecution operator that executes
// queries across distributed queriers. It implements Thanos engine's logical plan execution by:
//  1. Streaming series metadata to discover the data shape
//  2. Fetching actual data values via subsequent Next calls
//
// Unlike local execution, this operator retrieves data from remote querier processes,
// enabling distributed query processing across multiple nodes.
func newDistributedRemoteExecution(ctx context.Context, pool *client.Pool, fragmentKey FragmentKey, queryOpts *query.Options) (*DistributedRemoteExecution, error) {

	_, _, _, childIDToAddr, _ := ExtractFragmentMetaData(ctx)

	poolClient, err := pool.GetClientFor(childIDToAddr[fragmentKey.fragmentID])

	if err != nil {
		return nil, err
	}

	client, ok := poolClient.(*querierClient)
	if !ok {
		return nil, fmt.Errorf("invalid client type from pool")
	}

	d := &DistributedRemoteExecution{
		client: client,

		mint:        queryOpts.Start.UnixMilli(),
		maxt:        queryOpts.End.UnixMilli(),
		step:        queryOpts.Step.Milliseconds(),
		currentStep: queryOpts.Start.UnixMilli(),
		numSteps:    queryOpts.NumSteps(),

		batchSize:   1000,
		fragmentKey: fragmentKey,
		addr:        childIDToAddr[fragmentKey.fragmentID],
		buffer:      []model.StepVector{},
		bufferIndex: 0,
		initialized: false,
	}

	if d.step == 0 {
		d.step = 1
	}

	return d, nil
}

func (d *DistributedRemoteExecution) Series(ctx context.Context) ([]labels.Labels, error) {

	if d.series != nil {
		return d.series, nil
	}

	req := &querierpb.SeriesRequest{
		QueryID:    d.fragmentKey.queryID,
		FragmentID: d.fragmentKey.fragmentID,
		Batchsize:  d.batchSize,
	}

	stream, err := d.client.Series(ctx, req)
	if err != nil {
		return nil, err
	}

	var series []labels.Labels

	for {
		seriesBatch, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		for _, s := range seriesBatch.OneSeries {
			oneSeries := make(map[string]string, len(s.Labels))
			for _, l := range s.Labels {
				oneSeries[l.Name] = l.Value
			}
			series = append(series, labels.FromMap(oneSeries))
		}
	}

	d.series = series
	return series, nil
}

func (d *DistributedRemoteExecution) Next(ctx context.Context) ([]model.StepVector, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if d.currentStep > d.maxt {
		return nil, nil
	}

	ts := d.currentStep
	numVectorsNeeded := 0
	for currStep := 0; currStep < d.numSteps && ts <= d.maxt; currStep++ {
		numVectorsNeeded++
		ts += d.step
	}

	// return from buffer first
	if d.buffer != nil && d.bufferIndex < len(d.buffer) {
		end := d.bufferIndex + int(d.numSteps)
		if end > len(d.buffer) {
			end = len(d.buffer)
		}
		result := d.buffer[d.bufferIndex:end]
		d.bufferIndex = end

		if d.bufferIndex >= len(d.buffer) {
			d.buffer = nil
			d.bufferIndex = 0
		}

		return result, nil
	}

	// initialize stream if haven't
	if !d.initialized {
		req := &querierpb.NextRequest{
			QueryID:    d.fragmentKey.queryID,
			FragmentID: d.fragmentKey.fragmentID,
			Batchsize:  d.batchSize,
		}
		stream, err := d.client.Next(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize stream: %w", err)
		}
		d.stream = stream
		d.initialized = true
	}

	// get new batch from server
	batch, err := d.stream.Recv()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error receiving from stream: %w", err)
	}

	// return new batch and save it
	d.buffer = make([]model.StepVector, len(batch.StepVectors))
	for i, sv := range batch.StepVectors {
		d.buffer[i] = model.StepVector{
			T:            sv.T,
			SampleIDs:    sv.Sample_IDs,
			Samples:      sv.Samples,
			HistogramIDs: sv.Histogram_IDs,
			Histograms:   floatHistogramProtoToFloatHistograms(sv.Histograms),
		}
	}

	end := d.numSteps
	if end > len(d.buffer) {
		end = len(d.buffer)
	}
	result := d.buffer[:end]
	d.bufferIndex = end

	if d.bufferIndex >= len(d.buffer) {
		d.buffer = nil
		d.bufferIndex = 0
	}

	d.currentStep += d.step * int64(len(result))

	return result, nil
}

func (d *DistributedRemoteExecution) Close() error {
	if d.stream != nil {

		if err := d.stream.CloseSend(); err != nil {
			return fmt.Errorf("error closing stream: %w", err)
		}
	}
	d.buffer = nil
	d.bufferIndex = 0
	d.initialized = false
	return nil
}

func (d DistributedRemoteExecution) GetPool() *model.VectorPool {
	return d.vectors
}

func (d DistributedRemoteExecution) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{}
}

func (d DistributedRemoteExecution) String() string {
	return "DistributedRemoteExecution(" + d.addr + ")"
}
