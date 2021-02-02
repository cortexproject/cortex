package thanosconvert

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

// ThanosBlockConverter converts blocks written by Thanos to make them readable by Cortex
type ThanosBlockConverter struct {
	logger log.Logger
	bkt    objstore.Bucket
	dryRun bool
}

type PerUserResults struct {
	FailedBlocks, ConvertedBlocks, UnchangedBlocks []string
}

func NewPerUserResults() PerUserResults {
	return PerUserResults{
		FailedBlocks:    []string{},
		ConvertedBlocks: []string{},
		UnchangedBlocks: []string{},
	}
}

func (r *PerUserResults) AddFailed(blockID string) {
	r.FailedBlocks = append(r.FailedBlocks, blockID)
}
func (r *PerUserResults) AddConverted(blockID string) {
	r.ConvertedBlocks = append(r.ConvertedBlocks, blockID)
}

func (r *PerUserResults) AddUnchanged(blockID string) {
	r.UnchangedBlocks = append(r.UnchangedBlocks, blockID)
}

type Results map[string]PerUserResults

// NewThanosBlockConverter creates a ThanosBlockConverter
func NewThanosBlockConverter(ctx context.Context, cfg bucket.Config, dryRun bool, logger log.Logger) (*ThanosBlockConverter, error) {
	bkt, err := bucket.NewClient(ctx, cfg, "thanosconvert", logger, nil)
	if err != nil {
		return nil, err
	}

	return &ThanosBlockConverter{
		bkt:    bkt,
		logger: logger,
		dryRun: dryRun,
	}, err
}

// Run iterates over all blocks from all users in a bucket
func (c ThanosBlockConverter) Run(ctx context.Context) (Results, error) {
	results := make(Results)

	scanner := cortex_tsdb.NewUsersScanner(c.bkt, cortex_tsdb.AllUsers, c.logger)
	users, _, err := scanner.ScanUsers(ctx)
	if err != nil {
		return results, errors.Wrap(err, "error while scanning users")
	}
	level.Info(c.logger).Log("msg", "Scanned tenants")

	for _, u := range users {
		r, err := c.convertUser(ctx, u)
		results[u] = r
		if err != nil {
			return results, errors.Wrap(err, fmt.Sprintf("error converting user %s", u))
		}
	}
	return results, nil
}

func (c ThanosBlockConverter) convertUser(ctx context.Context, user string) (PerUserResults, error) {
	results := PerUserResults{}

	userBucketClient := bucket.NewUserBucketClient(user, c.bkt)
	err := userBucketClient.Iter(ctx, "", func(o string) error {
		// get block ID from path
		o = strings.TrimSuffix(o, "/")
		blockULID, err := ulid.Parse(o)
		if err != nil {
			// this is not a block, skip it
			return nil
		}
		blockID := blockULID.String()

		level.Debug(c.logger).Log("msg", "Processing block", "block", blockID)

		// retrieve meta.json

		metaKey := fmt.Sprintf("%s/%s/meta.json", user, blockID)
		r, err := c.bkt.Get(ctx, metaKey)
		if err != nil {
			level.Error(c.logger).Log("msg", "Get meta.json", "block", blockID, "meta_key", metaKey, "err", err.Error())
			results.AddFailed(blockID)
			return nil
		}

		meta, err := metadata.Read(r)
		if err != nil {
			level.Error(c.logger).Log("msg", "decode meta.json into metadata.Meta", "block", blockID, "meta_key", metaKey, "err", err.Error())
			results.AddFailed(blockID)
			return nil
		}

		// convert and upload if appropriate

		newMeta, changesRequired := convertMetadata(*meta, user)

		if len(changesRequired) > 0 {
			if c.dryRun {
				level.Debug(c.logger).Log("msg", "Block requires changes, not uploading due to dry run", "block", blockID, "changes_required", strings.Join(changesRequired, ","))
			} else {
				level.Debug(c.logger).Log("msg", "Block requires changes, uploading new meta.json", "block", blockID, "changes_required", strings.Join(changesRequired, ","))
				if err := c.uploadNewMeta(ctx, userBucketClient, blockID, newMeta); err != nil {
					level.Error(c.logger).Log("msg", "Update meta.json", "block", blockID, "meta_key", metaKey, "err", err.Error())
					results.AddFailed(blockID)
					return nil
				}
			}
			results.AddConverted(blockID)
		} else {
			results.AddUnchanged(blockID)
		}

		return nil
	})
	if err != nil {
		return results, err
	}

	return results, nil
}

func (c *ThanosBlockConverter) uploadNewMeta(ctx context.Context, userBucketClient *bucket.UserBucketClient, blockID string, meta metadata.Meta) error {
	var body bytes.Buffer
	if err := meta.Write(&body); err != nil {
		return errors.Wrap(err, "encode json")
	}

	if err := userBucketClient.Upload(ctx, fmt.Sprintf("%s/meta.json", blockID), &body); err != nil {
		return errors.Wrap(err, "upload to bucket")
	}
	return nil
}

func convertMetadata(meta metadata.Meta, expectedUser string) (metadata.Meta, []string) {
	var changesRequired []string

	if meta.Thanos.Labels == nil {
		// TODO: no "thanos" entry could mean this block is corrupt - should we bail out instead?
		meta.Thanos.Labels = map[string]string{}
		// don't need to add to changesRequired, since the code below will notice lack of org id
	}

	org, ok := meta.Thanos.Labels[cortex_tsdb.TenantIDExternalLabel]
	if !ok {
		changesRequired = append(changesRequired, "add __org_id__ label")
	} else if org != expectedUser {
		changesRequired = append(changesRequired, fmt.Sprintf("change __org_id__ from %s to %s", org, expectedUser))
	}

	// remove __org_id__ so that we can see if there are any other labels
	delete(meta.Thanos.Labels, cortex_tsdb.TenantIDExternalLabel)
	if len(meta.Thanos.Labels) > 0 {
		changesRequired = append(changesRequired, "remove extra Thanos labels")
	}

	meta.Thanos.Labels = map[string]string{
		cortex_tsdb.TenantIDExternalLabel: expectedUser,
	}

	return meta, changesRequired
}
