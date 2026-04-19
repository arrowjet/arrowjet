# IAM Setup Guide

Arrowjet's bulk mode requires an IAM role that both Redshift and the driver can use to access S3.

## Model 1: Same Account, Same Region (Simplest)

The Redshift cluster and S3 bucket are in the same AWS account and region.

### Step 1: Create an IAM role for Redshift

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-staging-bucket",
        "arn:aws:s3:::your-staging-bucket/*"
      ]
    }
  ]
}
```

### Step 2: Attach the role to your Redshift cluster

```bash
aws redshift modify-cluster-iam-roles \
  --cluster-identifier your-cluster \
  --add-iam-roles arn:aws:iam::123456789:role/RedshiftS3Role
```

### Step 3: Configure Arrowjet

```python
conn = arrowjet.connect(
    host="your-cluster.region.redshift.amazonaws.com",
    staging_bucket="your-staging-bucket",
    staging_iam_role="arn:aws:iam::123456789:role/RedshiftS3Role",
    staging_region="us-east-1",
    ...
)
```

## Model 2: Same Account, Separate Bucket Ownership

The staging bucket is managed by a different team.

Add a bucket policy granting the Redshift role access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789:role/RedshiftS3Role"
      },
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::their-bucket",
        "arn:aws:s3:::their-bucket/arrowjet-staging/*"
      ]
    }
  ]
}
```

## Model 3: Cross-Account Bucket

The bucket is in a different AWS account.

Requires:
1. Bucket policy with cross-account principal
2. Redshift role with `sts:AssumeRole` or direct cross-account S3 access
3. Driver-side credentials with cross-account access

Note: set `disallow_cross_region=False` if the bucket is in a different region (not recommended for performance).

## KMS Encryption

If using SSE-KMS, add KMS permissions to the IAM role:

```json
{
  "Effect": "Allow",
  "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": "arn:aws:kms:us-east-1:123456789:key/your-key-id"
}
```

Configure in Arrowjet:

```python
conn = arrowjet.connect(
    ...
    staging_encryption="sse_kms",
    staging_kms_key_id="arn:aws:kms:us-east-1:123456789:key/your-key-id",
)
```
