package thanosconvert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
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
func NewThanosBlockConverter(ctx context.Context, cfg bucket.Config, dryRun bool, logger log.Logger, reg prometheus.Registerer) (*ThanosBlockConverter, error) {
	bkt, err := bucket.NewClient(ctx, cfg, "thanosconvert", logger, reg)
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

	// discover users in the bucket
	var users []string
	err := c.bkt.Iter(ctx, "", func(o string) error {
		users = append(users, strings.TrimSuffix(o, "/"))
		return nil
	})
	if err != nil {
		return results, err
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

	err := c.bkt.Iter(ctx, user, func(o string) error {
		// get block ID from path

		parts := strings.Split(o, "/")
		if len(parts) != 3 {
			// this seems pretty bad, let's bail out
			return errors.Errorf("couldn't parse bucket path %s as {user}/{block}/", o)
		}
		blockID := parts[1]
		level.Debug(c.logger).Log("msg", "Processing block", "block", blockID)

		// retrieve meta.json

		metaKey := fmt.Sprintf("%s/%s/meta.json", user, blockID)
		r, err := c.bkt.Get(ctx, metaKey)
		if err != nil {
			level.Error(c.logger).Log("msg", "Get meta.json", "block", blockID, "meta_key", metaKey, "err", err.Error())
			results.AddFailed(blockID)
			return nil
		}

		meta := &metadata.Meta{}
		decoder := json.NewDecoder(r)
		err = decoder.Decode(meta)
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
				if err := c.uploadNewMeta(ctx, user, blockID, newMeta); err != nil {
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

func (c *ThanosBlockConverter) uploadNewMeta(ctx context.Context, user, blockID string, meta metadata.Meta) error {
	key := fmt.Sprintf("%s/%s/meta.json", user, blockID)

	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	if err := enc.Encode(meta); err != nil {
		return errors.Wrap(err, "encode json")
	}

	if err := c.bkt.Upload(ctx, key, &body); err != nil {
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

	org, ok := meta.Thanos.Labels["__org_id__"]
	if !ok {
		changesRequired = append(changesRequired, "add __org_id__ label")
	} else if org != expectedUser {
		changesRequired = append(changesRequired, fmt.Sprintf("change __org_id__ from %s to %s", org, expectedUser))
	}

	// remove __org_id__ so that we can see if there are any other labels
	delete(meta.Thanos.Labels, "__org_id__")
	if len(meta.Thanos.Labels) > 0 {
		changesRequired = append(changesRequired, "remove extra Thanos labels")
	}

	meta.Thanos.Labels = map[string]string{
		"__org_id__": expectedUser,
	}

	return meta, changesRequired
}
