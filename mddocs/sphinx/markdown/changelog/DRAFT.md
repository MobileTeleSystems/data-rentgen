# 0.4.0 [UNRELEASED] (2025-08-15)

## Features

- Add custom `dataRentgen_run` and `dataRentgen_operation` facets allowing to:
  : * Passing custom `external_id`, `persistent_log_url` and other fields of Run.
    * Passing custom `name`, `description`, `group`, `positition` fields of Operation.
    * mark event as containing only Operation or both Run + Operation data. ([#265](https://github.com/MobileTeleSystems/data-rentgen/issues/265))
- Add new entities `Tag` and `TagValue`. Tags can be used as additional properties for another entities.
  Add tags for dataset.
  > ### Response examples

  > Before:
  > ```python
  > {
  >     "nodes": {
  >         "datasets": {
  >             "8400": {
  >                 "id": "8400",
  >                 "location": {...},
  >                 "name": "dataset_name",
  >                 "schema": {},
  >         }

  >     },
  >     "relations": {...}
  > ```

  > After:
  > ```python
  > {
  >     "nodes": {
  >         "datasets": {
  >             "8400": {
  >                 "id": "25896",
  >                 "location": {...},
  >                 "name": "dataset_name",
  >                 "schema": {...},
  >                 "tags": {  # <---
  >                     {
  >                         "name": "environment",
  >                         "value": "production"

  >                     },
  >                     {
  >                         "name": "team",
  >                         "value": "my_awesome_team"

  >                     },
  >                 },

  >             }
  >         }
  >         ...
  >     },
  >     "relations": {...} (:issue:`268`)
  > ```
- Add REST API endpoints for managing personal tokens:
  - `GET /personal-tokens` - get personal tokens for current user.
  - `POST /personal-tokens` - create new personal token for current user.
  - `PATCH /personal-tokens/:id` - refresh personal token (revoke token and create new one).
  - `DELETE /personal-tokens/:id` - revoke personal token. ([#276](https://github.com/MobileTeleSystems/data-rentgen/issues/276))
- Introduce new HTTP2Kafka component. It allows using DataRentgen with OpenLineage HttpTransport.
  Authentication is done using personal tokens. ([#281](https://github.com/MobileTeleSystems/data-rentgen/issues/281))

## Improvements

- Add workaround if OpenLineage emitted Spark application event with `job.name=unknown`.

  This requires installing OpenLineage with this fix merged: [https://github.com/OpenLineage/OpenLineage/pull/3848](https://github.com/OpenLineage/OpenLineage/pull/3848). ([#263](https://github.com/MobileTeleSystems/data-rentgen/issues/263))
- Dataset symlinks with no inputs/outputs are no longer removed from lineage graph. ([#269](https://github.com/MobileTeleSystems/data-rentgen/issues/269))

## Bug Fixes

- If some run uses the same table as both input and output (e.g. merging duplicates or performing some checks before writing),
  DataRentgen excludes `dataset1 -> dataset1` relations from lineage with `granularity=DATASET`. ([#261](https://github.com/MobileTeleSystems/data-rentgen/issues/261))
- In 0.3.0 and 0.3.1 lineage with `granularity=DATASET` may return datasets which were not interacted with each other,
  but were inputs/outputs of the same run. Now only direct interactions are used while resolving lineage graph. ([#264](https://github.com/MobileTeleSystems/data-rentgen/issues/264))
