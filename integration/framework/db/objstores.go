package e2edb

import e2e "github.com/cortexproject/cortex/integration/framework"

const (
	MinioAccessKey = "Cheescake"
	MinioSecretKey = "supersecret"
)

// NewMinio returns minio server, used as a local replacement for S3.
func NewMinio() *e2e.Service {
	return e2e.NewService(
		"minio",
		// If you change the image tag, remember to update it in the preloading done
		// by CircleCI too (see .circleci/config.yml).
		"minio/minio:RELEASE.2019-12-30T05-45-39Z",
		e2e.NetworkName,
		[]int{9000},
		map[string]string{
			"MINIO_ACCESS_KEY": MinioAccessKey,
			"MINIO_SECRET_KEY": MinioSecretKey,
			"MINIO_BROWSER":    "off",
			"ENABLE_HTTPS":     "0",
		},
		// Create the "cortex" bucket before starting minio
		e2e.NewCommandWithoutEntrypoint("sh", "-c", "mkdir -p /data/e2e-minio && minio server --quiet /data"),
		e2e.NewReadinessProbe(9000, "/minio/health/ready", 200),
	)
}
