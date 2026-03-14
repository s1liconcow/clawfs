# ClawFS Storage Modes

## Summary
- ClawFS should support two storage modes:
  - `hosted_free`: provider-owned tiny hosted bucket for zero-setup onboarding.
  - `byob_paid`: user-owned bucket or bucket prefix for durable production usage.
- The object-store data path should stay direct from client to object store after bootstrap. ClawFS should not proxy steady-state reads and writes.
- Hosted free-tier limits should be enforced in the control plane and credential issuance path, not inside ClawFS core.

## Hosted Free Tier
- Use a provider-owned object bucket with one unique prefix per account and volume, for example `free/<account_id>/<volume_id>/`.
- Issue short-lived object credentials scoped to exactly one hosted prefix.
- Return bucket, endpoint or region, prefix, credential expiry, and capped runtime settings to the launcher or client.
- Keep hosted storage intentionally small and temporary:
  - hard cap on active volumes per account
  - hard cap on logical bytes per volume
  - hard cap on total hosted bytes per account
  - hard cap on retained checkpoints or snapshots
  - hard cap on runtime knobs such as `pending_bytes` and `segment_cache_bytes`
- Hosted free-tier volumes should auto-expire after inactivity. Expired prefixes should be deleted asynchronously by a cleanup job, and new credentials should not be issued for expired volumes.

## Bring Your Own Bucket
- Paid durable usage should default to user-owned object storage.
- The user or their organization should control the bucket, billing boundary, and long-lived IAM policy.
- ClawFS should accept bucket and prefix configuration plus scoped user credentials or assumed-role credentials and then access the object store directly.
- In BYOB mode, quota enforcement remains primarily a cloud-account and IAM concern. ClawFS should not try to meter or proxy the user's bucket traffic invisibly.

## Permissions Model
- Prefer prefix-scoped temporary credentials over request-by-request signed URL brokering.
- Hosted free-tier credentials must only allow access to a single volume prefix.
- Cross-prefix access should be impossible via issued credentials alone.
- BYOB mode should recommend a dedicated bucket prefix per ClawFS account or volume to keep IAM simple and auditable.
- The control plane should store connection metadata and issuance state, not raw object payloads.

## Runtime Contract
- The launcher should receive enough object configuration to start ClawFS directly:
  - object provider
  - bucket
  - region or endpoint
  - object prefix
  - temporary credentials and expiry
  - plan-capped config values for cache, pending bytes, and retention-sensitive features
- Credential refresh should renew access to the same prefix without changing volume identity.

## Upgrade Path
- Free hosted storage is for onboarding and experimentation.
- Paid durable usage should migrate to BYOB.
- Migration should copy or checkpoint the hosted prefix into the user-owned bucket and then switch the control-plane volume record to `byob_paid`.
- The client-visible volume identity should stay stable across that migration.

## Repo Notes
- Today, ClawFS already supports the object-store primitives needed for this model:
  - `bucket`
  - `object_prefix`
  - provider selection in `src/config.rs`
- The repo does not yet include a control plane, quota service, temporary credential issuer, or hosted-prefix lifecycle manager.
- Until those components exist, this document is the source of truth for ClawFS storage/auth product behavior.
