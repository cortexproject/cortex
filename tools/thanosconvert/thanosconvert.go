package thanosconvert

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
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
	level.Info(c.logger).Log("msg", "Scanned users")

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

	// No per-tenant config provider because the thanosconvert tool doesn't support it.
	userBucketClient := bucket.NewUserBucketClient(user, c.bkt, nil)

	err := userBucketClient.Iter(ctx, "", func(o string) error {
		blockID, ok := block.IsBlockDir(o)
		if !ok {
			// not a block
			return nil
		}

		// retrieve meta.json

		meta, err := block.DownloadMeta(ctx, c.logger, userBucketClient, blockID)
		if err != nil {
			level.Error(c.logger).Log("msg", "download block meta", "block", blockID.String(), "user", user, "err", err.Error())
			results.AddFailed(blockID.String())
			return nil
		}

		// convert and upload if appropriate

		newMeta, changesRequired := convertMetadata(meta, user)

		if len(changesRequired) > 0 {
			if c.dryRun {
				level.Info(c.logger).Log("msg", "Block requires changes (dry-run)", "block", blockID.String(), "user", user, "changes_required", strings.Join(changesRequired, ","))
			} else {
				level.Info(c.logger).Log("msg", "Block requires changes, uploading new meta.json", "block", blockID.String(), "user", user, "changes_required", strings.Join(changesRequired, ","))
				if err := c.uploadNewMeta(ctx, userBucketClient, blockID.String(), newMeta); err != nil {
					level.Error(c.logger).Log("msg", "Update meta.json", "block", blockID.String(), "user", user, "err", err.Error())
					results.AddFailed(blockID.String())
					return nil
				}
			}
			results.AddConverted(blockID.String())
		} else {
			level.Info(c.logger).Log("msg", "Block doesn't need changes", "block", blockID.String(), "user", user)
			results.AddUnchanged(blockID.String())
		}

		return nil
	})
	if err != nil {
		return results, err
	}

	return results, nil
}

func (c *ThanosBlockConverter) uploadNewMeta(ctx context.Context, userBucketClient objstore.Bucket, blockID string, meta metadata.Meta) error {
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
