---
description: Move Greenhouse recruiting data from the Harvest API into Estuary, including candidates, applications, jobs, offers, interviews, and users.
---

# Greenhouse

This connector captures data from [Greenhouse](https://www.greenhouse.com/) into Estuary collections.

## Supported data resources

The following data resources are supported through the Greenhouse Harvest API:

| Resource | Replication Mode |
| --- | --- |
| [applications](https://harvestdocs.greenhouse.io/reference/get_v3-applications) | Incremental |
| [application_stages](https://harvestdocs.greenhouse.io/reference/get_v3-application-stages) | Incremental |
| [applied_candidate_tags](https://harvestdocs.greenhouse.io/reference/get_v3-applied-candidate-tags) | Incremental |
| [approval_flows](https://harvestdocs.greenhouse.io/reference/get_v3-approval-flows) | Incremental |
| [approver_groups](https://harvestdocs.greenhouse.io/reference/get_v3-approver-groups) | Incremental |
| [approvers](https://harvestdocs.greenhouse.io/reference/get_v3-approvers) | Incremental |
| [attachments](https://harvestdocs.greenhouse.io/reference/get_v3-attachments) | Incremental |
| [candidates](https://harvestdocs.greenhouse.io/reference/get_v3-candidates) | Incremental |
| [candidate_attribute_types](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-attribute-types) | Incremental |
| [candidate_educations](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-educations) | Incremental |
| [candidate_employments](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-employments) | Incremental |
| [candidate_tags](https://harvestdocs.greenhouse.io/reference/get_v3-candidate-tags) | Incremental |
| [close_reasons](https://harvestdocs.greenhouse.io/reference/get_v3-close-reasons) | Incremental |
| [custom_fields](https://harvestdocs.greenhouse.io/reference/get_v3-custom-fields) | Incremental |
| [custom_field_options](https://harvestdocs.greenhouse.io/reference/get_v3-custom-field-options) | Incremental |
| [demographic_answers](https://harvestdocs.greenhouse.io/reference/get_v3-demographic-answers) | Incremental |
| [demographic_questions](https://harvestdocs.greenhouse.io/reference/get_v3-demographic-questions) | Snapshot |
| [demographic_question_sets](https://harvestdocs.greenhouse.io/reference/get_v3-demographic-question-sets) | Incremental |
| [departments](https://harvestdocs.greenhouse.io/reference/get_v3-departments) | Incremental |
| [eeoc](https://harvestdocs.greenhouse.io/reference/get_v3-eeoc) | Incremental |
| [email_templates](https://harvestdocs.greenhouse.io/reference/get_v3-email-templates) | Incremental |
| [interviews](https://harvestdocs.greenhouse.io/reference/get_v3-interviews) | Incremental |
| [interviewers](https://harvestdocs.greenhouse.io/reference/get_v3-interviewers) | Incremental |
| [interviewer_tags](https://harvestdocs.greenhouse.io/reference/get_v3-interviewer-tags) | Incremental |
| [job_hiring_managers](https://harvestdocs.greenhouse.io/reference/get_v3-job-hiring-managers) | Incremental |
| [job_interview_stages](https://harvestdocs.greenhouse.io/reference/get_v3-job-interview-stages) | Incremental |
| [job_notes](https://harvestdocs.greenhouse.io/reference/get_v3-job-notes) | Incremental |
| [job_posts](https://harvestdocs.greenhouse.io/reference/get_v3-job-posts) | Incremental |
| [jobs](https://harvestdocs.greenhouse.io/reference/get_v3-jobs) | Incremental |
| [offers](https://harvestdocs.greenhouse.io/reference/get_v3-offers) | Incremental |
| [offices](https://harvestdocs.greenhouse.io/reference/get_v3-offices) | Incremental |
| [openings](https://harvestdocs.greenhouse.io/reference/get_v3-openings) | Incremental |
| [prospect_pools](https://harvestdocs.greenhouse.io/reference/get_v3-prospect-pools) | Incremental |
| [prospect_pool_stages](https://harvestdocs.greenhouse.io/reference/get_v3-prospect-pool-stages) | Incremental |
| [referrers](https://harvestdocs.greenhouse.io/reference/get_v3-referrers) | Incremental |
| [rejection_details](https://harvestdocs.greenhouse.io/reference/get_v3-rejection-details) | Incremental |
| [rejection_reasons](https://harvestdocs.greenhouse.io/reference/get_v3-rejection-reasons) | Incremental |
| [scorecard_questions](https://harvestdocs.greenhouse.io/reference/get_v3-scorecard-questions) | Incremental |
| [scorecards](https://harvestdocs.greenhouse.io/reference/get_v3-scorecards) | Incremental |
| [sources](https://harvestdocs.greenhouse.io/reference/get_v3-sources) | Incremental |
| [user_emails](https://harvestdocs.greenhouse.io/reference/get_v3-user-emails) | Incremental |
| [user_job_permissions](https://harvestdocs.greenhouse.io/reference/get_v3-user-job-permissions) | Incremental |
| [user_roles](https://harvestdocs.greenhouse.io/reference/get_v3-user-roles) | Incremental |
| [users](https://harvestdocs.greenhouse.io/reference/get_v3-users) | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.

## Prerequisites

* A set of Greenhouse Harvest V3 (OAuth) API credentials. See [Greenhouse's documentation](https://harvestdocs.greenhouse.io/docs/authentication#custom-integrations-oauth-20-client-credentials-step-by-step) for instructions on how to generate these credentials.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Greenhouse source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/client_id`** | Client ID | The Client ID from your Greenhouse API credentials. | string | Required |
| **`/credentials/client_secret`** | Client Secret | The Client secret from your Greenhouse API credentials. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Set to `Client Credentials`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string |  |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs | string |          |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-greenhouse-native:v1
        config:
          credentials:
            client_id: <secret>
            client_secret: <secret>
            access_token: <secret>
          start_date: 2026-03-30T12:00:00Z
    bindings:
      - resource:
          name: application_stages
        target: ${PREFIX}/application_stages
```
