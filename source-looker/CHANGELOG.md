# Changelog

## 2026-07-26

### Fixed
- The `lookml_model_explores` binding no longer fails when a LookML model advertises an
  explore that Looker won't serve, such as an abstract explore or one in a model that no
  longer compiles. These explores are now skipped with a warning.
- The `user_roles`, `user_credentials_embed`, and `user_attribute_values` bindings no
  longer fail when a user becomes unavailable partway through a capture.
