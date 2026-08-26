# Changelog

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
