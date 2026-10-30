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
  backpressure, and from an object that holds as many appends as its store
  allows (`SegmentLimitException`); `maxAppendBytes(location)` tells a caller
  the most one append may write.
- **Cloud-provider implementations** of that interface for AWS S3 (standard +
  Express One Zone), GCS (standard + Rapid Storage zonal buckets), and Azure
  ADLS Gen2.
- **`SupportsCatalogTransactions` / `BaseCatalogTransaction`** — multi-table
  transaction interfaces in `iceberg-core`, wired into `RESTCatalog` and reused
  by the external `FileIOCatalog`. `commitTransaction` takes the transaction's
  read set: each table it writes, as it first read it, and under serializable
  isolation each branch it read, so a catalog can fail a transaction whose
  tables changed after it read them. `RESTCatalog` does not send the read set;
  its server checks only the table commits' requirements.
- **Inline manifest lists** — `ManifestListSink`, a `TableOperations` mix-in
  to which `SnapshotProducer` hands each commit's manifest-list delta instead
  of writing an Avro manifest list, and `InlineSnapshot`, a snapshot that
  holds its list in memory (`manifestListLocation()` is `null`).
  `RewriteTablePathUtil` rejects such snapshots.
- **Commit footprints** — `CommitFootprint`: what each snapshot producer's
  commit depends on (BLIND, VALIDATED with the checks its retry would make, or
  OPAQUE), attached to the commit's `InlineSnapshot`, so a catalog can merge a
  commit onto a table that changed after it was read where Iceberg's retry
  would commit the same.
- **`SerializableTable` for tables without a metadata file** — a table whose
  metadata location is not a `metadata.json` (an inline table's) is shipped
  with its metadata as JSON and its inline snapshots' manifests, so that a
  Spark or Flink worker that deserializes it, by Java serialization or Kryo,
  reads the table's snapshots, history and metadata tables.
- **`TableMetadata.Builder` additions** — `setLastUpdatedMillis`, so a catalog
  replaying a commit keeps the writer's `last-updated-ms`, and
  `replaceSnapshots`, which swaps snapshot implementations (e.g. for
  `InlineSnapshot`) on load.
- **Abstract test suites** (`CatalogTransactionTests`, atomic-FileIO contract
  tests) published as a `tests` classifier JAR for downstream consumers.

`atomicio` sits on branch `core-fixes`: one upstream-style commit for each of
the Iceberg core bugs C2–C27, with its tests. The companion catalog's fuzzers
found C2–C14, C24, C26 and C27; a review of Iceberg's commit retry paths that
their finds prompted found C15–C23, and a review of the catalog's
conflict-encoding plan C25. C23, C24 and C25 follow open upstream pull
requests, C26 one the stale bot closed, and C27 completes an earlier fix. The
bugs are documented in that repository's
[`docs/ICEBERG_ISSUES.md`](https://github.com/cdouglas/catalog/blob/main/docs/ICEBERG_ISSUES.md),
with a document per issue for upstream reports in
[`docs/iceberg-issues/`](https://github.com/cdouglas/catalog/tree/main/docs/iceberg-issues).

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

## Testing on real cloud storage

`SupportsAtomicOperationsContractTest` (in `iceberg-api`'s test jar) is the
contract every `SupportsAtomicOperations` implementation must meet: racing
creates, replaces and appends have exactly one winner, whose bytes are stored;
a CAS that fails mid-write leaves its target intact; a write is conditioned on
the version its snapshot has seen, and a snapshot that has seen none assumes no
object (a CAS from it only creates); an append to a version that a CAS replaced
with an object of the same length fails; and an append past the store's limit
on writes per object fails with `SegmentLimitException`
([`docs/docs/atomic_io.md`](docs/docs/atomic_io.md) "Snapshots"). A
subclass per store runs it against real storage, and each case skips when its
store's environment is not set:

| Test | Store | Environment |
|---|---|---|
| `S3StandardFileIOAtomicTest` | S3 general-purpose bucket (CAS only) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_TEST_BUCKET` |
| `S3ExpressFileIOAtomicTest` | S3 Express One Zone directory bucket | the same, with `S3_EXPRESS_TEST_BUCKET` |
| `GcsFileIOAtomicTest` | GCS standard bucket | `GOOGLE_APPLICATION_CREDENTIALS` (a service-account key); the test's `TEST_BUCKET` constant names both its bucket and its project |
| `GcsFileIOAtomicRapidTest`, `GcsRapidCanaries` | GCS zonal (Rapid Storage) bucket | `RAPID_BUCKET`, application default credentials, and optionally `GOOGLE_CLOUD_PROJECT` |
| `AdlsFileIOAtomicTest`, `AdlsFileIOVendedCredentialsTest` | ADLS Gen2 account with a hierarchical namespace | `AZURE_STORAGE_ACCOUNT`, `AZURE_TEST_CONTAINER`, `AZURE_STORAGE_SAS_TOKEN` (with its leading `?`) |

`GcsRapidCanaries` checks the zonal-bucket behavior `GCSFileIO` relies on, and
`AdlsFileIOVendedCredentialsTest` a CAS and an append with the SAS token
vended by a (mock) REST catalog. One ADLS case lists its directory and skips
when the SAS token is object-scoped (`srt=o`).

`appendPastTheSegmentLimitFails` is a canary, not part of a regular run. It
checks only that the store's error at its limit on writes per object maps to
`SegmentLimitException`, which changes only if the store's behavior does, and
it makes 10,000 appends on S3 Express and 50,000 on ADLS. It runs with
`ATOMIC_SEGMENT_LIMIT_TEST=true` and skips otherwise. The companion catalog
tests how it reacts to the exception on an in-memory store with a small limit.

```bash
./gradlew \
  :iceberg-aws:test --rerun --tests '*.S3StandardFileIOAtomicTest' --tests '*.S3ExpressFileIOAtomicTest' \
  :iceberg-gcp:test --rerun --tests '*.GcsFileIOAtomicTest' --tests '*.GcsFileIOAtomicRapidTest' \
    --tests '*.GcsRapidCanaries' \
  :iceberg-azure:test --rerun --tests '*.AdlsFileIOAtomicTest' --tests '*.AdlsFileIOVendedCredentialsTest'
```

`--rerun` matters: Gradle's up-to-date checks and build cache don't see
environment variables, so without it Gradle can report the results of an
earlier run in which every case skipped. `GcsCasTransportProbe` and
`GcsCasViaRewriteProbe` are a record of the GCS write-path survey in
[`docs/docs/atomic_io_gcs_rapid.md`](docs/docs/atomic_io_gcs_rapid.md), not
part of the contract; two of `GcsCasViaRewriteProbe`'s cases fail on a zonal
bucket by design.

The companion catalog runs Iceberg's `CatalogTests` and
`CatalogTransactionTests` on the same stores (its `docs/INTEGRATION_TESTING.md`).

For all other build, module, and engine-compatibility documentation, refer to
upstream Apache Iceberg: <https://iceberg.apache.org/docs/latest/>.
