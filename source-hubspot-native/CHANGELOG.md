# Changelog

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
