package chunk

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/cortex/pkg/prom1/storage/local/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const userID = "userID"

func dummyChunk(now model.Time) Chunk {
	return dummyChunkFor(now, model.Metric{
		model.MetricNameLabel: "foo",
		"bar":  "baz",
		"toms": "code",
	})
}

func dummyChunkFor(now model.Time, metric model.Metric) Chunk {
	cs, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := NewChunk(
		userID,
		metric.Fingerprint(),
		metric,
		cs[0],
		now.Add(-time.Hour),
		now,
	)
	// Force checksum calculation.
	_, err := chunk.Encode()
	if err != nil {
		panic(err)
	}
	return chunk
}

func TestChunkCodec(t *testing.T) {
	dummy := dummyChunk(model.Now())
	decodeContext := NewDecodeContext()
	for i, c := range []struct {
		chunk Chunk
		err   error
		f     func(*Chunk, []byte)
	}{
		// Basic round trip
		{chunk: dummy},

		// Checksum should fail
		{
			chunk: dummy,
			err:   ErrInvalidChecksum,
			f:     func(_ *Chunk, buf []byte) { buf[4]++ },
		},

		// Checksum should fail
		{
			chunk: dummy,
			err:   ErrInvalidChecksum,
			f:     func(c *Chunk, _ []byte) { c.Checksum = 123 },
		},

		// Metadata test should fail
		{
			chunk: dummy,
			err:   ErrWrongMetadata,
			f:     func(c *Chunk, _ []byte) { c.Fingerprint++ },
		},

		// Metadata test should fail
		{
			chunk: dummy,
			err:   ErrWrongMetadata,
			f:     func(c *Chunk, _ []byte) { c.UserID = "foo" },
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			buf, err := c.chunk.Encode()
			require.NoError(t, err)

			have, err := ParseExternalKey(userID, c.chunk.ExternalKey())
			require.NoError(t, err)

			if c.f != nil {
				c.f(&have, buf)
			}

			err = have.Decode(decodeContext, buf)
			require.Equal(t, c.err, errors.Cause(err))

			if c.err == nil {
				require.Equal(t, have, c.chunk)
			}
		})
	}
}

func TestChunkCodecCompat(t *testing.T) {
	for _, c := range []struct {
		chunk  Chunk
		data   string
		length int
	}{
		// Binary data encoded with code from commit 864c53a5b82a
		{
			chunk:  dummyChunk(model.Time(1521894448000)),
			data:   "\x00\x00\x00\xb7\xff\x06\x00\x00sNaPpY\x01\xa5\x00\x00\x0e%\xd8\x01{\"fingerprint\":18245339272195143978,\"userID\":\"userID\",\"from\":1521890848,\"through\":1521894448,\"metric\":{\"__name__\":\"foo\",\"bar\":\"baz\",\"toms\":\"code\"},\"encoding\":1}\n\x00\x00\x04\x00\x15\x00\x01\x00\x01\x80[\xfaWb\x01",
			length: 1211,
		},
		{
			chunk:  benchmarkChunk(model.Time(1521894448000)),
			data:   "\x00\x00\x02\xf6\xff\x06\x00\x00sNaPpY\x00\xe4\x02\x00(\x8c\x16\x1c\xc1\a\xf0O{\"fingerprint\":2979418513780949645,\"userID\":\"userID\",\"from\":1521890848,\"through\"\r\x15\x0444\x01\x15\xf0Xmetric\":{\"__name__\":\"container_cpu_usage_seconds_total\",\"beta_kubernetes_io_arch\":\"amd64\"R\"\x000instance_type\x01i(3.somesize\"R1\x00,os\":\"linux\",\x1d\x97\x01\xaa\b\":\"\x01<\x00-\x05\f\x10,\"cpu\x01Q\fpu01\x01\xfe4ailure_domain_J\xad\x00\x14region\rE\x14where-\x969\x00\bzon\x11|\r7\xf0\x8bb\",\"id\":\"/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28\",\"imag\x01\x9b\x01\xdbDstry/organisation/!(\xf0L@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506\",\"in)\xcb$\":\"ip-111-\x01\x038-11.ec2.internaA)\x14job\":\"!\x1a@rnetes-cadvisor\",\x1d\x16\x1c_io_host\x01\xa6BS\x00L\",\"monitor\":\"prod\",\"\r)\x18k8s_somI\a\t\n\x10otherE\x17\x14-5j8s8E\xa6\x1c-system_\x8e\x8c\x01\x04_0\r`\fspac\x01\x8e\x01\xbd\rA\x14\",\"pod2\x81\x02>p\x00@\"},\"encoding\":1}\n\x00\x00\x04\x00\x15\x00\x01\x00\x01\x80[\xfaWb\x01",
			length: 1786,
		},
	} {
		buf := make([]byte, c.length)
		copy(buf, c.data)

		// If we wanted to check encoding is still byte-for-byte the same:
		//check, err := c.chunk.Encode()
		//require.NoError(t, err)
		//require.Equal(t, buf, check)

		// Create a blank chunk with the right key and checksum
		have, err := ParseExternalKey(userID, c.chunk.ExternalKey())
		require.NoError(t, err)

		decodeContext := NewDecodeContext()
		err = have.Decode(decodeContext, buf)
		require.NoError(t, err)
		require.Equal(t, c.chunk, have)
	}
}

func TestParseExternalKey(t *testing.T) {
	for _, c := range []struct {
		key   string
		chunk Chunk
		err   error
	}{
		{key: "2:1484661279394:1484664879394", chunk: Chunk{
			UserID:      userID,
			Fingerprint: model.Fingerprint(2),
			From:        model.Time(1484661279394),
			Through:     model.Time(1484664879394),
		}},

		{key: userID + "/2:270d8f00:270d8f00:f84c5745", chunk: Chunk{
			UserID:      userID,
			Fingerprint: model.Fingerprint(2),
			From:        model.Time(655200000),
			Through:     model.Time(655200000),
			ChecksumSet: true,
			Checksum:    4165752645,
		}},

		{key: "invalidUserID/2:270d8f00:270d8f00:f84c5745", chunk: Chunk{}, err: ErrWrongMetadata},
	} {
		chunk, err := ParseExternalKey(userID, c.key)
		require.Equal(t, c.err, errors.Cause(err))
		require.Equal(t, c.chunk, chunk)
	}
}

func TestChunksToMatrix(t *testing.T) {
	// Create 2 chunks which have the same metric
	metric := model.Metric{
		model.MetricNameLabel: "foo",
		"bar":  "baz",
		"toms": "code",
	}
	now := model.Now()
	chunk1 := dummyChunkFor(now, metric)
	chunk1Samples, err := chunk1.Samples(chunk1.From, chunk1.Through)
	require.NoError(t, err)
	chunk2 := dummyChunkFor(now, metric)
	chunk2Samples, err := chunk2.Samples(chunk2.From, chunk2.Through)
	require.NoError(t, err)

	ss1 := &model.SampleStream{
		Metric: chunk1.Metric,
		Values: util.MergeSampleSets(chunk1Samples, chunk2Samples),
	}

	// Create another chunk with a different metric
	otherMetric := model.Metric{
		model.MetricNameLabel: "foo2",
		"bar":  "baz",
		"toms": "code",
	}
	chunk3 := dummyChunkFor(now, otherMetric)
	chunk3Samples, err := chunk3.Samples(chunk3.From, chunk3.Through)
	require.NoError(t, err)

	ss2 := &model.SampleStream{
		Metric: chunk3.Metric,
		Values: chunk3Samples,
	}

	for _, c := range []struct {
		chunks         []Chunk
		expectedMatrix model.Matrix
	}{
		{
			chunks:         []Chunk{},
			expectedMatrix: model.Matrix{},
		}, {
			chunks: []Chunk{
				chunk1,
				chunk2,
				chunk3,
			},
			expectedMatrix: model.Matrix{
				ss1,
				ss2,
			},
		},
	} {
		matrix, err := chunksToMatrix(context.Background(), c.chunks, chunk1.From, chunk3.Through)
		require.NoError(t, err)

		sort.Sort(matrix)
		sort.Sort(c.expectedMatrix)
		require.Equal(t, c.expectedMatrix, matrix)
	}
}

func benchmarkChunk(now model.Time) Chunk {
	// This is a real example from Kubernetes' embedded cAdvisor metrics, lightly obfuscated
	return dummyChunkFor(now, model.Metric{
		model.MetricNameLabel:              "container_cpu_usage_seconds_total",
		"beta_kubernetes_io_arch":          "amd64",
		"beta_kubernetes_io_instance_type": "c3.somesize",
		"beta_kubernetes_io_os":            "linux",
		"container_name":                   "some-name",
		"cpu":                              "cpu01",
		"failure_domain_beta_kubernetes_io_region": "somewhere-1",
		"failure_domain_beta_kubernetes_io_zone":   "somewhere-1b",
		"id":       "/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28",
		"image":    "registry/organisation/name@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506",
		"instance": "ip-111-11-1-11.ec2.internal",
		"job":      "kubernetes-cadvisor",
		"kubernetes_io_hostname": "ip-111-11-1-11",
		"monitor":                "prod",
		"name":                   "k8s_some-name_some-other-name-5j8s8_kube-system_6e91c467-e4c5-11e7-ace3-0a97ed59c75e_0",
		"namespace":              "kube-system",
		"pod_name":               "some-other-name-5j8s8",
	})
}

func BenchmarkEncode(b *testing.B) {
	chunk := dummyChunk(model.Now())

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		chunk.Encode()
	}
}

func BenchmarkDecode1(b *testing.B)     { benchmarkDecode(b, 1) }
func BenchmarkDecode100(b *testing.B)   { benchmarkDecode(b, 100) }
func BenchmarkDecode10000(b *testing.B) { benchmarkDecode(b, 10000) }

func benchmarkDecode(b *testing.B, batchSize int) {
	chunk := benchmarkChunk(model.Now())
	buf, err := chunk.Encode()
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decodeContext := NewDecodeContext()
		b.StopTimer()
		chunks := make([]Chunk, batchSize)
		// Copy across the metadata so the check works out ok
		for j := 0; j < batchSize; j++ {
			chunks[j] = chunk
			chunks[j].Metric = nil
			chunks[j].Data = nil
		}
		b.StartTimer()
		for j := 0; j < batchSize; j++ {
			err := chunks[j].Decode(decodeContext, buf)
			require.NoError(b, err)
		}
	}
}
