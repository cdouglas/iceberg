<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Iceberg `atomicio` — atomic FileIO operations on top of Apache Iceberg 1.12.0

This is a fork of [Apache Iceberg](https://iceberg.apache.org) 1.12.0 that adds
the primitives needed to build a *storage-only* Iceberg catalog: one whose state
lives entirely in object storage and which serializes commits through the
conditional-write primitives the underlying store already provides
(`if-match`/`if-generation-match`/`if-none-match`). No external metastore, no
coordination service.

The companion catalog implementation that consumes these primitives lives in a
separate repository: <https://github.com/cdouglas/catalog>.

## What this fork adds

- **`SupportsAtomicOperations`** — a `FileIO` extension exposing atomic
  compare-and-swap (CAS) and offset-pinned append (APPEND) writes, with
  structured exceptions distinguishing precondition failures from transient
  backpressure.
- **Cloud-provider implementations** of that interface for AWS S3 (standard +
  Express One Zone), GCS (standard + Rapid Storage zonal buckets), and Azure
  ADLS Gen2.
- **`SupportsCatalogTransactions` / `BaseCatalogTransaction`** — multi-table
  transaction interfaces in `iceberg-core`, wired into `RESTCatalog` and reused
  by the external `FileIOCatalog`.
- **Inline manifest lists** — `ManifestListSink`, a `TableOperations` mix-in
  to which `SnapshotProducer` hands each commit's manifest-list delta instead
  of writing an Avro manifest list, and `InlineSnapshot`, a snapshot that
  holds its list in memory (`manifestListLocation()` is `null`).
  `RewriteTablePathUtil` rejects such snapshots.
- **`TableMetadata.Builder` additions** — `setLastUpdatedMillis`, so a catalog
  replaying a commit keeps the writer's `last-updated-ms`, and
  `replaceSnapshots`, which swaps snapshot implementations (e.g. for
  `InlineSnapshot`) on load.
- **Abstract test suites** (`CatalogTransactionTests`, atomic-FileIO contract
  tests) published as a `tests` classifier JAR for downstream consumers.

`atomicio` sits on branch `core-fixes`: one upstream-style commit for each of
the Iceberg core bugs C2–C7 that the companion catalog's fuzzers found, with
its tests. The bugs are documented in that repository's
`docs/ICEBERG_ISSUES.md`.

The atomic primitives, the per-provider preconditions, the ADLS Append Blob
writes (APPEND is one conditional `AppendBlock`; CAS renames a staged temp onto
the target), and the GCS zonal-bucket write paths (CAS is one XML API PUT) are
documented in detail in [`docs/docs/atomic_io.md`](docs/docs/atomic_io.md) and
[`docs/docs/atomic_io_gcs_rapid.md`](docs/docs/atomic_io_gcs_rapid.md).

## Motivation

Cloud object stores already provide the only piece of coordination an Iceberg
catalog needs: conditional writes. A catalog that targets that primitive can
collapse the metastore tier into a single object whose mutations are
serialized by the storage layer itself, while still supporting atomic
multi-table transactions. `SupportsAtomicOperations` is the contract that lets
the catalog implementation stay storage-agnostic, and the per-provider
implementations cover the corner cases — same-position concurrent appends on
ADLS, zonal buckets on GCS Rapid, create races on S3 — that don't survive a
naive "write with if-match" approach.

## Building

This fork builds with the upstream Gradle setup (Java 17 or 21). Most
downstream work consumes it via `publishToMavenLocal`:

```bash
./gradlew publishToMavenLocal -x test -x integrationTest
```

It publishes the version of the release it is based on, marked as the fork's:
`1.12.0-atomicio-SNAPSHOT` on Iceberg 1.12.0. Don't skip `generateGitProperties`
(it works in git worktrees here): without it the jars carry a stale
`iceberg-build.properties`, and snapshot summaries name the wrong Iceberg
version.

Cloud-provider integration tests under `iceberg-aws`, `iceberg-gcp`, and
`iceberg-azure` require credentials or emulators; they skip cleanly when none
are available.

For all other build, module, and engine-compatibility documentation, refer to
upstream Apache Iceberg: <https://iceberg.apache.org/docs/latest/>.
