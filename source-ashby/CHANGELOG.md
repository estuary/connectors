# Changelog

## 2026-07-27

### Changed
- The `archive_reasons`, `interview_stages`, `job_postings`, and `sources` collections are
  now discovered with a minimal write schema instead of declaring a required `id` string.
  Documents are still captured as Ashby returns them, and existing captures are unaffected.
