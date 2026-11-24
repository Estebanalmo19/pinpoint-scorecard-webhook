# Pinpoint API discovery (2026-11-24)

Source of truth: OpenAPI spec embedded in the reference pages, spec version `v1.0.27`.
Legend: **[SPEC]** verified in the OpenAPI JSON · **[DOC]** stated in docs/changelog prose ·
**[PENDING]** not documented, must be validated against the live API (read-only).

## Base, auth, format

| Item | Value | Source |
|---|---|---|
| Base URL | `https://{subdomain}.pinpointhq.com/api/v1` | [DOC] docs/introduction |
| Auth | Header `X-API-KEY: <key>` | [SPEC] securitySchemes.ApiKeyAuth, docs/authentication |
| Format | JSON:API, `application/vnd.api+json`; append `.json` for plain JSON | [SPEC]/[DOC] |
| Key permissions | Per-resource read/write/delete; which permission covers scorecards | [PENDING] |
| Errors | `401` missing/invalid key, `403` "Integration not enabled", `400`, `404`, `422`, `429`, `500` | [SPEC] |

## Endpoints

### `GET /api/v1/applications` — [SPEC] reference/get-applications
- Filters: `filter[updated_at][gt|lt]`, `filter[created_at][gt|lt]`, `filter[uuid]`, `filter[candidate_id]`,
  `filter[job_id]`, `filter[job_posting_id]`, `filter[stage_id]`, `filter[stage_basic_type]`
  (`applied|rejected|review|interview|hired|offer|sourced`), `filter[job_visibility]`
  (`internal|external|confidential|private_job`), `filter[external_system_reference]`,
  `filter[internal_candidate]`, `filter[locale]`, `filter[candidate_email]`, `filter[candidate_phone]`, `filter[concealed]`.
- Sort: `id`, `created_at`, `updated_at` (prefix `-` for desc).
- Pagination: `page[number]` (default 1), `page[size]` (default 100, max 1000), `stats[total]=count`.
- `include`: … `stage`, `stage_changes`, `user_scorecards`, `scorecard_ratings`, `scorecard_notes`, `scorecard_answers` …
- Sparse fieldsets: `fields[applications]`, `fields[user_scorecards]`, `fields[stages]`, …
- Attributes of interest: `updated_at`, `created_at`, `stage_id`, `rejected_at`, `hired_at`, `job_id`, `candidate_id`.
  No `status` attribute.
- Default exclusions [DOC]: confidential-job applications (unless `filter[job_visibility]` includes `confidential`)
  and `sourced` stage (unless `filter[stage_basic_type]=sourced`).

### `GET /api/v1/applications/{id}` — [DOC] reference/get-application (Fetch Application)

### `GET /api/v1/scorecards` and `/{id}` — [SPEC] reference/get-scorecards
- Scorecard = **template** (`name`, `interview_guidance`, `is_active`, `scorecardable_type`, timestamps).
- Filters: `filter[updated_at][gt|lt]`, `filter[created_at][gt|lt]`, `filter[scorecardable_type]`.
- Sort: `id` only. `include`: `scorecard_notes`, `scorecard_sections`, `user_scorecards`, `stages`.

### User Scorecards (individual submissions)
- Schema `user_scorecards` [SPEC]: `completed_at` (date-time), `locked` (bool), `name`, `sentiment`, `status`,
  `summary`, `created_at`, `updated_at` — all read-only, **no enums** for `status`/`sentiment`.
- Relationships [SPEC]: `scorecard`, `user`, `scorecard_note`, `scorecard_answers`, `scorecard_ratings`.
  **No `application` relationship** — the application link only exists when sideloaded from the application.
- Direct endpoint `api/v1/user_scorecards` is mentioned in changelog 1.0.27 but has **no reference page** [PENDING].
- Documented access: `GET /applications?include=user_scorecards`, `GET /scorecards?include=user_scorecards`.
- Changelog Nov 2026: sideloading scorecard resources from applications now returns only that application's own scorecards.

### Stage change (NOT to be used yet) — [DOC] reference/put-application
`PUT /api/v1/applications/{id}` with `relationships.stage.data = {type: "stages", id}`.

## Webhooks [DOC] docs/webhooks-overview
Events: Applicant hired, Application moved, Application stage changed, Background check completed,
Document request accepted, Interview scheduled, Job created/deleted/updated, New application, Offer accepted,
Onboarding completed, Onboarding stage completed (v2), Onboarding workflow completed, Requisition approved/created,
Talent Pipeline candidate created, User created on company.
**No scorecard event.** 5 s timeout, 10 retries over 24 h, then auto-disable.

**Signature** [DOC] docs/verifying-webhooks *(corrected 2026-11-24; the first discovery wrongly said it was undocumented)*:
each request carries the header `PINPOINT-HMAC-SHA256` = Base64( HMAC-SHA256( signing_secret, raw_request_body ) ).
The signing secret is in Settings > Webhooks. Verify with a constant-time comparison (`hmac.compare_digest`) over the raw body bytes.
Not documented: timestamp/replay protection and secret rotation. Receiving webhooks is out of scope for now.

## Not documented — validate live (phase 2 tooling: `pinpoint-scorecards`)
1. Rate limit numbers and headers (only the `429` body is documented).
2. Whether `application.updated_at` changes when a user scorecard is completed (**critical** for the incremental design).
3. Allowed values of `user_scorecards.status` and `sentiment`.
4. Whether `GET /api/v1/user_scorecards` is callable and which filters it accepts.
5. Response `meta`/`links` keys for pagination.
6. Which API key permission grants access to user scorecards.
7. Whether `completed_at` is reset or changed if a scorecard is reopened/edited (`locked` semantics).

## Live observations (read-only, 2026-11-24)

- `user_scorecards.status` observed values: `complete`, `incomplete`. `sentiment` observed: `positive`, null.
- A test scorecard went from `complete`/`positive` to `incomplete`/null with `completed_at` unchanged, and the
  parent application's `updated_at` did not change. The cause (edit, reset) is not observable through the API.
- Consequences: completion is keyed on `status`, not `completed_at`; and `filter[updated_at][gt]` on applications
  cannot be relied on to detect scorecard changes, so discovery compares full snapshots of user scorecards.
