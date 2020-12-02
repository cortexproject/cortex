package bucketindex

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"path"
	"strings"

	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type globalMarkersBucket struct {
	objstore.Bucket
}

// BucketWithGlobalMarkers wraps the input bucket into a bucket which also keeps track of markers
// in the global markers location.
func BucketWithGlobalMarkers(b objstore.Bucket) objstore.Bucket {
	return &globalMarkersBucket{
		Bucket: b,
	}
}

// Upload implements objstore.Bucket.
func (b *globalMarkersBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	blockID, ok := b.isBlockDeletionMark(name)
	if !ok {
		return b.Bucket.Upload(ctx, name, r)
	}

	// Read the marker.
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	// Upload it to the original location.
	if err := b.Bucket.Upload(ctx, name, bytes.NewReader(body)); err != nil {
		return err
	}

	// Upload it to the global markers location too.
	globalMarkPath := path.Clean(path.Join(path.Dir(name), "../", BlockDeletionMarkFilepath(blockID)))
	return b.Bucket.Upload(ctx, globalMarkPath, bytes.NewReader(body))
}

// Delete implements objstore.Bucket.
func (b *globalMarkersBucket) Delete(ctx context.Context, name string) error {
	// Call the parent.
	if err := b.Bucket.Delete(ctx, name); err != nil {
		return err
	}

	// Delete the marker in the global markers location too.
	if blockID, ok := b.isBlockDeletionMark(name); ok {
		globalMarkPath := path.Clean(path.Join(path.Dir(name), "../", BlockDeletionMarkFilepath(blockID)))
		if err := b.Bucket.Delete(ctx, globalMarkPath); err != nil {
			if !b.IsObjNotFoundErr(err) {
				return err
			}
		}
	}

	return nil
}

func (b *globalMarkersBucket) isBlockDeletionMark(name string) (ulid.ULID, bool) {
	if path.Base(name) != metadata.DeletionMarkFilename {
		return ulid.ULID{}, false
	}

	// Parse the block ID in the path. If there's not block ID, then it's not the per-block
	// deletion mark.
	parts := strings.Split(name, "/")
	if len(parts) < 2 {
		return ulid.ULID{}, false
	}

	return block.IsBlockDir(parts[len(parts)-2])
}
