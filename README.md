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

# Iceberg `atomicio` — atomic FileIO operations on top of Apache Iceberg 1.10.1

This is a fork of [Apache Iceberg](https://iceberg.apache.org) 1.10.1 that adds
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
- **Abstract test suites** (`CatalogTransactionTests`, atomic-FileIO contract
  tests) published as a `tests` classifier JAR for downstream consumers.

The atomic primitives, the per-provider preconditions, the ADLS append-flush
lease serialization, and the GCS Rapid stage-and-move CAS path are documented
in detail in [`docs/docs/atomic_io.md`](docs/docs/atomic_io.md) and
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

This fork builds with the upstream Gradle setup (Java 11, 17, or 21). Most
downstream work consumes it via `publishToMavenLocal`:

```bash
./gradlew publishToMavenLocal -x test -x integrationTest -x generateGitProperties
```

Cloud-provider integration tests under `iceberg-aws`, `iceberg-gcp`, and
`iceberg-azure` require credentials or emulators; they skip cleanly when none
are available.

For all other build, module, and engine-compatibility documentation, refer to
upstream Apache Iceberg: <https://iceberg.apache.org/docs/latest/>.
