# Pinecone S3 storage integration

Bulk import from a private bucket needs a storage integration: an IAM role in the customer's
account that Pinecone assumes to read the import prefix. Resolves the "Storage integration"
open question in `docs/design.md`: the integration can be created through the API, so the
runbook has no manual console step unless the customer prefers one.

## 1. IAM role

`pcmigrate provision` creates the role as part of the base stack (`infra/base`). Its trust
policy names Pinecone's AWS account, `713131977538`, and its policy grants `s3:ListBucket`,
`s3:GetBucketLocation`, `s3:GetObject`, and `s3:PutObject` on the migration prefix only.

Pinecone's documentation does not require an external id. When the customer's security team
wants one, set `pinecone_storage_external_id` in the run config and the trust policy adds the
`sts:ExternalId` condition.

## 2. Register the integration

Through the API, using the project's API key:

```sh
curl -s -X POST https://api.pinecone.io/storage-integrations \
  -H "Api-Key: $PINECONE_API_KEY" \
  -H "X-Pinecone-Api-Version: unstable" \
  -H "Content-Type: application/json" \
  -d '{"name":"<migration_id>","provider":"s3","aws_iam_role":{"role_arn":"<role arn from provision outputs>"}}'
```

The response carries `id` and `status`. `status` is `Validated` when Pinecone could assume the
role. Put `id` into `storage_integration_id` in the run config; the import stage passes it to
every `start_import` call.

Through the console: Manage, Storage integrations, Add integration, Amazon S3, paste the
role ARN.

Verified on 2026-10-01 against the live API: the endpoint is still on the `unstable` version
and the provider value must be `s3`, `gcs`, or `azure_blob`. Re-check when it graduates to a
dated API version.

## 3. Verify before import

```sh
curl -s https://api.pinecone.io/storage-integrations \
  -H "Api-Key: $PINECONE_API_KEY" -H "X-Pinecone-Api-Version: unstable"
```

Every entry for the migration should show `"status":"Validated"`. Anything else means the
trust policy or bucket policy is wrong, and `start_import` will fail.
