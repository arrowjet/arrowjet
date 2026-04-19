# Configuration Reference

## connect() Parameters

### Redshift Connection

| Parameter | Type | Default | Description |
|---|---|---|---|
| `host` | str | required | Redshift cluster endpoint |
| `database` | str | `"dev"` | Database name |
| `user` | str | `"awsuser"` | Database user |
| `password` | str | `""` | Database password |
| `port` | int | `5439` | Redshift port |

### S3 Staging (required for bulk mode)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `staging_bucket` | str | None | S3 bucket for staging files |
| `staging_iam_role` | str | None | IAM role ARN for COPY/UNLOAD |
| `staging_region` | str | None | AWS region (must match cluster) |
| `staging_prefix` | str | `"arrowjet-staging"` | S3 key prefix |
| `staging_cleanup` | str | `"on_success"` | Cleanup policy (see below) |
| `staging_encryption` | str | `"none"` | Encryption mode (see below) |
| `staging_kms_key_id` | str | None | KMS key ARN (required if encryption=sse_kms) |

### Concurrency & Safety

| Parameter | Type | Default | Description |
|---|---|---|---|
| `max_concurrent_bulk_ops` | int | `4` | Max parallel bulk operations per connection |
| `max_staging_bytes` | int | `10GB` | Max staged data per operation |
| `disallow_cross_region` | bool | `True` | Reject if bucket region != cluster region |
| `s3_endpoint_url` | str | None | S3 endpoint override (for VPC endpoints) |

## Cleanup Policies

| Policy | Behavior |
|---|---|
| `"always"` | Delete staged files after every operation |
| `"on_success"` | Delete on success, preserve on failure for debugging |
| `"never"` | User manages lifecycle (manual cleanup) |
| `"ttl_managed"` | Rely on S3 lifecycle rules to expire files |

## Encryption Modes

| Mode | Description |
|---|---|
| `"none"` | No encryption on staged files |
| `"sse_s3"` | S3-managed encryption (AES-256) |
| `"sse_kms"` | KMS-managed encryption (requires kms_key_id) |

## Environment Variables

For CI/CD and testing, credentials can be set via environment variables:

```bash
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=dev
REDSHIFT_USER=awsuser
REDSHIFT_PASS=your-password
STAGING_BUCKET=your-bucket
STAGING_IAM_ROLE=arn:aws:iam::123:role/RedshiftS3
STAGING_REGION=us-east-1
```
