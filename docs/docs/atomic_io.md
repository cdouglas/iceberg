# Atomic FileIO Operations

This document describes the `SupportsAtomicOperations` interface and its implementations in the GCS, S3, and Azure FileIO providers.

## Overview

Cloud object stores provide conditional write primitives that enable atomic updates:
- **GCS**: Generation numbers and `if-generation-match` preconditions
- **S3**: ETags and `if-match` preconditions (with S3 Express One Zone write offset)
- **Azure ADLS**: ETags and `If-Match` request conditions

The `SupportsAtomicOperations` interface exposes these primitives through a unified API, enabling catalog implementations to perform atomic multi-table transactions without external coordination services.

## Core Interfaces

### SupportsAtomicOperations

```java
public interface SupportsAtomicOperations extends FileIO {
    /**
     * Create an atomic output file that can conditionally replace an existing file.
     */
    AtomicOutputFile newOutputFile(InputFile toReplace);

    /**
     * Whether this FileIO exposes the APPEND strategy. Backends without an append primitive
     * (e.g. GCS objects are immutable) return false; callers that mix CAS and APPEND must fall
     * back to CAS-only on those backends. Of the fork's implementations only GCSFileIO returns
     * false; see Limitations for standard S3 buckets.
     */
    default boolean supportsAppend() { return true; }

    /** Base class for atomic-operation failures. */
    class AtomicOperationException extends RuntimeException { ... }

    /** Atomic CAS write could not complete. Subclassed below. */
    class CASException extends AtomicOperationException { ... }

    /**
     * Storage rejected the write because its CAS precondition failed (etag/generation mismatch,
     * or a "must not exist" lost a create race). Retrying the same write will fail again — the
     * caller must re-read state and reconcile before another attempt.
     *
     * This is no proof that the write did not land: an SDK retry of a write whose reply was
     * lost fails its precondition on the version the first attempt wrote. A read that does not
     * show the write rules it out.
     */
    class StorageInvariantException extends CASException { ... }

    /**
     * Storage applied backpressure (HTTP 429, 5xx, GCS per-object update rate). The caller may
     * retry the same write after a backoff: its precondition still guards it.
     *
     * The write may have landed nonetheless (a 5xx can follow an applied write; an SDK that
     * retries reports only its last attempt). A retry of a write that landed fails its
     * precondition; a caller that stops retrying must read the target to learn the outcome.
     */
    class StorageThrottleException extends CASException { ... }

    /** APPEND failed: the target changed since the snapshot (etag, or length / write offset). */
    class AppendException extends AtomicOperationException { ... }
}
```

### AtomicOutputFile

```java
public interface AtomicOutputFile extends OutputFile {
    enum Strategy { CAS, APPEND }

    /**
     * Prepare a checksum/token for atomic write.
     * This reads the source to compute checksums without writing.
     */
    CAS prepare(Supplier<InputStream> source, Strategy strategy) throws IOException;

    /**
     * Atomically write content, failing if the target changed since prepare().
     *
     * @param token Checksum from prepare()
     * @param source Content to write
     * @return InputFile representing the written object
     * @throws CASException if target was modified (CAS strategy)
     * @throws AppendException if offset mismatch (APPEND strategy)
     */
    InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException;
}
```

### Write Strategies

- **CAS (Compare-and-Swap)**: Replace the entire file atomically. Fails if the file was modified since the InputFile was read.
- **APPEND**: Append to an existing file at a specific offset. Fails if the file size changed (useful for log-structured formats).

## GCS Implementation

### GCSFileIO

Implements `SupportsAtomicOperations` using GCS generation numbers. Every CAS write is a single request: `storage.create` on standard buckets, an XML API PUT on zonal (Rapid Storage) buckets, which reject the JSON API's single-request and resumable uploads. `supportsAppend()` returns `false` — GCS objects are immutable, so APPEND has no honest implementation and the catalog format coerces its commit policy to CAS-only on this backend.

```java
public class GCSFileIO implements DelegateFileIO, SupportsAtomicOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        GCSInputFile gcsInputFile = (GCSInputFile) replace;
        PrefixedStorage storage = clientForStoragePath(replace.location());
        BlobId pinnedId = gcsInputFile.pinnedBlobId();
        boolean zonal = storage.isZonalBucket(gcsInputFile.blobId().getBucket());
        return GCSOutputFile.replacing(
            gcsInputFile.blobId(), pinnedId, storage.storage(), storage.gcsFileSystem(),
            storage.gcpProperties(), metrics,
            zonal ? storage.xmlPutWriter() : null,  // CAS: one XML API PUT
            zonal ? storage::grpcStorage : null);   // regular writes: appendable upload
    }

    @Override
    public boolean supportsAppend() {
        return false;
    }
}
```

### GCSOutputFile

`prepare()` rejects `Strategy.APPEND` outright and checksums the source (CRC32C). `writeAtomic()` buffers the payload, checks it against the prepared checksum, and sends it in one request:

```java
class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile {
    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
        byte[] payload = ByteStreams.toByteArray(source.get());
        // The SDK computes its own CRC32C over what it sends, so a source that changed since
        // prepare() is caught here, before anything is written.
        checkMatches(token, payload);
        if (zonalWriter != null) {
            // Zonal/Rapid: one XML API PUT with x-goog-if-generation-match and
            // x-goog-hash: crc32c=... See atomic_io_gcs_rapid.md.
            return zonalWriter.writeAtomic(
                blobId(), pinnedSnapshot, token.contentHeaderString(), payload,
                gcsFileSystem(), metrics());
        }
        // Standard buckets: one multipart objects.insert with ifGenerationMatch; the response
        // carries the new generation.
        return createAtomic(payload);
    }
}
```

### GCS Preconditions

- **Standard buckets, CAS**: `storage.create(..., BlobTargetOption.generationMatch(g))` — fails with 412 if the generation changed; `generationMatch(0)` requires that the object not exist
- **Rapid/zonal buckets, CAS**: XML API PUT with `x-goog-if-generation-match: g` — 412 on a stale generation, 400 on a CRC32C mismatch (see [atomic_io_gcs_rapid.md](atomic_io_gcs_rapid.md))
- **Throttling**: GCS limits mutations of a single object to about one per second (burst a few). Both bucket types return 429 past that; it maps to `StorageThrottleException`. A GCS catalog object therefore commits at most a few times per second whichever write path is used.
- **APPEND**: Rejected at `prepare()` with `IllegalArgumentException`. GCS objects are immutable.

### Regular writes on Rapid/zonal buckets

`createOrOverwrite()` (manifests, data files, metadata JSON) streams through `GCSOutputStream`, a JSON resumable upload, which zonal buckets reject; so do the XML API's resumable upload and the single-request create. On zonal buckets `GCSOutputFile` streams through `GCSAppendableOutputStream` instead: an appendable upload (gRPC `BidiWriteObject`, the only streaming write zonal buckets accept) to the final name, finalized on close. `close()` checks that the finalized object holds every byte written. The channel's `write()` is non-blocking and returns 0 when its outbound queue is full; the stream then waits for GCS to acknowledge what is queued (`flush()`) rather than spinning.

The object is visible, unfinalized, while it is written, and a writer that fails part way leaves the bytes it flushed. Iceberg reads only files a successful commit references, under names unique to each attempt, so such a file is an orphan, as a failed write leaves on other stores; it is not staged and moved, which would cost a request per file. `PrefixedStorage` builds the gRPC client on the first zonal write, from the same configuration as the HTTP client (or, for a caller-provided client, its credentials and project).

## S3 Implementation

### S3FileIO

Implements `SupportsAtomicOperations` using S3 ETags and conditional requests.

```java
public class S3FileIO implements CredentialSupplier, DelegateFileIO,
        SupportsAtomicOperations, SupportsRecoveryOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        String path = replace.location();
        if (replace instanceof S3InputFile) {
            // A null etag means the object did not exist when pinned; that translates
            // to ifNoneMatch=* on commit so a stale "doesn't exist" writer cannot
            // silently overwrite the winner of a create race.
            String etag = ((S3InputFile) replace).etag();
            boolean assertAbsent = etag == null;
            return S3OutputFile.fromLocation(
                path, clientForStoragePath(path), metrics, etag, assertAbsent);
        }
        return S3OutputFile.fromLocation(path, clientForStoragePath(path), metrics);
    }
}
```

### S3OutputFile

Uses `if-match` and `writeOffsetBytes` for atomic operations:

```java
public class S3OutputFile extends BaseS3File implements AtomicOutputFile {
    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
        S3Checksum checksum = new S3Checksum(howto);
        try (InputStream in = source.get();
             FileChecksumOutputStream chk = new FileChecksumOutputStream(
                 NullOutputStream.INSTANCE, checksum)) {
            ByteStreams.copy(in, chk);
        }
        return checksum;
    }

    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
        S3Checksum tok = (S3Checksum) token;
        switch (tok.getStrategy()) {
            case CAS:
                return replaceDestObj(tok, source);  // Uses if-match ETag
            case APPEND:
                return appendDestObj(tok, source);   // Uses writeOffsetBytes
            default:
                throw new UnsupportedOperationException();
        }
    }
}
```

### S3 Preconditions

- **CAS**: `PutObjectRequest.ifMatch(etag)` - fails with 412 PreconditionFailed or 409 ConditionalRequestConflict
- **APPEND**: `PutObjectRequest.writeOffsetBytes(offset)` - fails with InvalidWriteOffsetException (S3 Express One Zone only)

### S3 Checksums

S3 uses CRC32C checksums in the `x-amz-checksum-crc32c` header:

```java
class S3Checksum implements CAS, FileChecksum {
    private final CRC32C crc32c = new CRC32C();

    public String contentHeaderString() {
        byte[] bytes = ByteBuffer.allocate(4)
            .order(ByteOrder.BIG_ENDIAN)
            .putInt((int) crc32c.getValue())
            .array();
        return Base64.getEncoder().encodeToString(bytes);
    }
}
```

## Azure ADLS Implementation

Requires a hierarchical-namespace (ADLS Gen2) account. Atomic writes keep the target as an **Append Blob**, written through the Blob API, and replace it with a Data Lake **rename**.

### ADLSFileIO

```java
public class ADLSFileIO implements DelegateFileIO, SupportsAtomicOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        String path = replace.location();
        if (replace instanceof ADLSInputFile) {
            DataLakeRequestConditions conditions = ((ADLSInputFile) replace).conditions();
            long objLength = replace.exists() ? replace.getLength() : -1L;
            return new ADLSOutputFile(
                path, fileClient(path), azureProperties, objLength, conditions, metrics,
                this::fileClient);  // builds the CAS temp's client, with the SAS the rename needs
        }
        return new ADLSOutputFile(path, fileClient(path), azureProperties, metrics, this::fileClient);
    }
}
```

### ADLSOutputFile

| Strategy | Requests | Precondition | Loser sees |
|---|---|---|---|
| APPEND | `AppendBlock` (1) | `x-ms-blob-condition-appendpos` = snapshot length, `If-Match` = snapshot ETag, `Content-MD5` of the block | 412 `ConditionNotMet` / `AppendPositionConditionNotMet` → `AppendException` |
| CAS | create temp Append Blob, `AppendBlock` (one per ≤100 MiB), rename onto the target (3) | rename destination `If-Match` = snapshot ETag, or `If-None-Match: *` to create; source `If-Match` = temp ETag | 412 `ConditionNotMet` or 409 `PathAlreadyExists` → `CASException` |

- **APPEND** is a single conditional request: the service checks both conditions and applies the block in one step, so of N writers appending to the same snapshot exactly one commits, and the committed bytes are its own. No lease.
- **CAS** never touches the target until the rename, which is atomic on an HNS account. A writer that fails earlier leaves the target as it was (the temp is deleted in a `finally`); readers see the old file or the new one. The temp is a hidden sibling of the target (`.<name>.cas-<uuid>.tmp`), so the rename stays within one directory. Before the rename, the staged content's MD5 is checked against the prepared token, so a source that changed since `prepare()` is never published.
- The rename keeps the temp's ETag and returns it, so the returned `InputFile` pins the new ETag without a HEAD (which could observe a later writer).
- Append Blob writes go through a Blob API client built on the Data Lake client's own HTTP pipeline (credentials, policies, retries), at the blob endpoint.

Measured on a real account (2026-10-14, p50): APPEND 15 ms; CAS of 64 KiB 144 ms. Mixed races (4 appenders and 4 CAS writers on one snapshot, 10 rounds) and 12 rounds of 8 racing appenders had exactly one winner each time, with the winner's bytes stored.

### Why not the Data Lake API

The previous implementation used Data Lake writes. Both of its paths had problems the Blob API avoids:

- **CAS** with `DataLakeFileClient.uploadWithResponse` is three requests in every SDK version: `PUT ?resource=file` (conditional, and it truncates the file to 0 bytes), then append and flush, unconditioned. A writer that died after the first request left an empty file, and readers could see the file empty mid-CAS (both observed on a real account). `SupportsAtomicOperationsContractTest.casReplaceThatFailsMidWriteLeavesTargetIntact` reproduces it.
- **APPEND** with `appendWithResponse` + `flushWithResponse`: concurrent same-position appends replace each other's uncommitted bytes, independently of which flush wins the ETag race, so a successful flush did not prove *your* bytes committed. A blob lease held across the pair closed that gap at the cost of a lease (and a failed lease-acquiring append can still leave a lease behind).
- The two APIs do not mix on one file: a file written through the Data Lake API cannot be appended with `AppendBlock` (409 `InvalidBlobType`), and one written through the Blob API cannot be appended through the Data Lake API (409 `InvalidAppendOperation`). A CAS replaces such a file with an Append Blob.

### Limitations

- **SAS on the rename**: under SAS auth the rename must carry the SAS in `x-ms-rename-source`, which the service authorizes separately from the request. The SDK adds it for clients built with a SAS token (`adls.sas-token.*`); `VendedAzureSasCredentialPolicy` adds the vended token (without it, the rename fails with 403 `AuthorizationPermissionMismatch` whatever the token's permissions). Shared key and OAuth need nothing.
- **50,000 appends per file**: an Append Blob holds at most 50,000 blocks; the next `AppendBlock` fails with 409 `BlockCountExceedsLimit`, which `ADLSOutputFile` throws as `SegmentLimitException`. A CAS (compaction) starts a fresh blob.
- **Emulator**: Azurite has no Data Lake endpoint, so the CAS rename cannot run there; it does enforce the `AppendBlock` conditions.

### Azure Checksums

`ADLSChecksum` computes the content MD5 in `prepare()` (computed once; `MessageDigest.digest()` resets the digest). APPEND sends it as the block's `Content-MD5`, which the service checks; CAS compares it with the staged content before the rename.

## Usage Example

```java
// Read current state
SupportsAtomicOperations io = (SupportsAtomicOperations) catalog.io();
InputFile current = io.newInputFile("gs://bucket/catalog.state");
byte[] content = readAll(current);

// Modify state
byte[] newContent = modifyState(content);

// Atomic write
AtomicOutputFile atomic = io.newOutputFile(current);
CAS token = atomic.prepare(
    () -> new ByteArrayInputStream(newContent),
    AtomicOutputFile.Strategy.CAS);

try {
    InputFile written = atomic.writeAtomic(
        token,
        () -> new ByteArrayInputStream(newContent));
    // Success - state updated atomically
} catch (SupportsAtomicOperations.CASException e) {
    // Conflict - retry from read
}
```

## Error Handling

| Provider | CAS Failure | Append Failure |
|----------|-------------|----------------|
| GCS | 412 Precondition Failed → `StorageInvariantException`; 429/5xx → `StorageThrottleException` | N/A (`supportsAppend() == false`) |
| S3 standard | 412 PreconditionFailed, 409 ConditionalRequestConflict → `CASException` | N/A (writeOffsetBytes rejected) |
| S3 Express One Zone | 412 PreconditionFailed, 409 ConditionalRequestConflict → `CASException` | InvalidWriteOffsetException → `AppendException`; TooManyPartsException → `SegmentLimitException` |
| Azure ADLS | 412 ConditionNotMet or 409 PathAlreadyExists (rename) → `CASException` | 412 ConditionNotMet / AppendPositionConditionNotMet → `AppendException`; 409 BlockCountExceedsLimit → `SegmentLimitException` |

`CASException` is the base type for atomic-CAS failures; callers that need to distinguish "snapshot is stale, must reconcile" from "transient, retry the same bytes" can catch `StorageInvariantException` and `StorageThrottleException` separately. `AppendException` covers the APPEND-strategy failures a caller retries as an append (another writer appended first); `SegmentLimitException` is a separate type, because only a CAS can follow it.

## Limitations

1. **S3 APPEND**: Only supported on S3 Express One Zone (directory) buckets; standard buckets reject `writeOffsetBytes`. `S3FileIO.supportsAppend()` returns `true` for both, so a catalog on a standard bucket must not append: set `fileio.catalog.max.append.count=0` (CAS only) for `FileIOCatalog`. The standard-bucket contract test (`S3StandardFileIOAtomicTest`) overrides the contract test's `supportsAppend()` hook to skip the APPEND cases.
2. **GCS APPEND**: Not supported. GCS objects are immutable — every write replaces the whole object — so `GCSFileIO.supportsAppend()` returns `false` and `GCSOutputFile.prepare()` rejects `Strategy.APPEND` with `IllegalArgumentException`. Callers that mix CAS and APPEND in a commit log must fall back to CAS-only.
3. **ADLS APPEND**: One conditional `AppendBlock` per append; at most 50,000 appends per file between CAS compactions. CAS requires a hierarchical-namespace account (it renames).
4. **Writes per object**: an S3 Express object holds at most 10,000 parts and an Azure Append Blob 50,000 blocks, counting the write that creates or replaces it and each append. An append past the limit fails with `SupportsAtomicOperations.SegmentLimitException`, not an `AppendException`: nothing landed, and no append to the object can succeed, so the caller must replace it by CAS. `SupportsAtomicOperationsContractTest.appendPastTheSegmentLimitFails` checks this against the real store when run with `ATOMIC_SEGMENT_LIMIT_TEST=true` (it makes that many requests).
5. **Large Files**: Atomic operations work best for small files (metadata, catalog state); large writes may exceed provider request timeouts.
