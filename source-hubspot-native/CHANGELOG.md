# Changelog

## 2026-09-02
### Fixed
- The `form_submissions` binding could permanently skip submissions that arrived on a
  form while a sweep was still checking other forms. Each sweep now only emits
  submissions made at least five minutes before it started and checkpoints the newest
  one emitted, so anything newer is read by a later sweep.

### Changed
- The `form_submissions` binding now reads historical submissions through a dedicated
  backfill task that checkpoints after each form, so a restart resumes from the last
  completed form instead of re-reading every form's history in a single sweep.

## 2026-08-27
### Added
- New `leads` binding for the HubSpot Leads object. Leads requires a Sales Hub
  Professional or Enterprise subscription, and the binding is only discovered for accounts
  that grant the `crm.objects.leads.read` scope. Because HubSpot grants optional scopes at
  install time, existing OAuth captures must re-authorize before `leads` is discovered.

## 2026-08-04
### Fixed
- Sourced schemas now describe the `_meta` field.

## 2026-07-29
### Fixed
- The `forms` binding now captures `captured`, `flow`, and `blog_comment` forms in
  addition to the already captured `hubspot` forms.
- The `form_submissions` binding now captures submissions for `captured` and `flow`
  form in addition to submissions already captured for `hubspot` forms. Submissions
  for `blog_comment` forms are not captured because HubSpot responds with a 400 error
  when requesting submissions for those forms.
