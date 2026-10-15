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
     * (e.g. GCS objects are immutable; standard S3 buckets reject writeOffsetBytes) return
     * false; callers that mix CAS and APPEND must fall back to CAS-only on those backends.
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
     */
    class StorageInvariantException extends CASException { ... }

    /**
     * Storage applied backpressure (HTTP 429, 5xx, GCS per-object update rate). Underlying
     * invariants still hold; the caller should retry the same write after a backoff.
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

Implements `SupportsAtomicOperations` using GCS generation numbers, with a separate stage-and-move path for zonal (Rapid Storage) buckets. `supportsAppend()` returns `false` — GCS objects are immutable, so APPEND has no honest implementation and the catalog format coerces its commit policy to CAS-only on this backend.

```java
public class GCSFileIO implements DelegateFileIO, SupportsAtomicOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        GCSInputFile gcsInputFile = (GCSInputFile) replace;
        PrefixedStorage storage = clientForStoragePath(replace.location());
        BlobId pinnedId = gcsInputFile.pinnedBlobId();
        GCSRapidStageAndMove zonalWriter = null;
        if (storage.isZonalBucket(gcsInputFile.blobId().getBucket())) {
            zonalWriter = new GCSRapidStageAndMove(
                storage.grpcStorage(), storage.gcpProperties(), metrics);
        }
        return GCSOutputFile.replacing(
            gcsInputFile.blobId(), pinnedId, storage.storage(),
            storage.gcpProperties(), metrics, zonalWriter);
    }

    @Override
    public boolean supportsAppend() {
        return false;
    }
}
```

### GCSOutputFile

`prepare()` rejects `Strategy.APPEND` outright. `writeAtomic()` branches on bucket type:

```java
class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile {
    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
        Preconditions.checkArgument(
            howto == Strategy.CAS,
            "GCS does not support append-mode atomic writes; use Strategy.CAS (got %s)",
            howto);
        GCSChecksum checksum = new GCSChecksum();
        try (InputStream in = source.get();
             FileChecksumOutputStream chk = new FileChecksumOutputStream(
                 ByteStreams.nullOutputStream(), checksum)) {
            ByteStreams.copy(in, chk);
        }
        return checksum;
    }

    @Override
    public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
        if (zonalWriter != null) {
            // Rapid/zonal: write to UUID temp via appendable upload, then moveBlob
            // with source+target generation preconditions. See atomic_io_gcs_rapid.md.
            byte[] payload = ByteStreams.toByteArray(source.get());
            return zonalWriter.writeAtomic(
                blobId(), pinnedSnapshot, token.contentHeaderString(), payload);
        }
        // Standard buckets: blobWriteSession with generationMatch precondition.
        ...
    }
}
```

### GCS Preconditions

- **Standard buckets, CAS**: `BlobWriteOption.generationMatch(expectedGeneration)` — fails with 412 if generation changed
- **Rapid/zonal buckets, CAS**: stage-and-move via `Storage.moveBlob` with source and target generation preconditions (see [atomic_io_gcs_rapid.md](atomic_io_gcs_rapid.md))
- **APPEND**: Rejected at `prepare()` with `IllegalArgumentException`. GCS objects are immutable.

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

- **Vended SAS**: under SAS auth the rename must carry the SAS in `x-ms-rename-source`. The SDK adds it for clients built with a SAS token (`adls.sas-token.*`), shared key or OAuth, but not for SAS added by `VendedAzureSasCredentialPolicy`, so CAS fails with 403 under vended credentials.
- **50,000 appends per file**: an Append Blob holds at most 50,000 blocks; the next `AppendBlock` fails with 409 `BlockCountExceedsLimit`. A CAS (compaction) starts a fresh blob.
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
| S3 Express One Zone | 412 PreconditionFailed, 409 ConditionalRequestConflict → `CASException` | InvalidWriteOffsetException → `AppendException` |
| Azure ADLS | 412 ConditionNotMet or 409 PathAlreadyExists (rename) → `CASException` | 412 ConditionNotMet / AppendPositionConditionNotMet → `AppendException` |

`CASException` is the base type for atomic-CAS failures; callers that need to distinguish "snapshot is stale, must reconcile" from "transient, retry the same bytes" can catch `StorageInvariantException` and `StorageThrottleException` separately. `AppendException` covers all APPEND-strategy failures.

## Limitations

1. **S3 APPEND**: Only supported on S3 Express One Zone (directory) buckets; standard buckets reject `writeOffsetBytes`. Provider tests declare this via `supportsAppend()`.
2. **GCS APPEND**: Not supported. GCS objects are immutable — every write replaces the whole object — so `GCSFileIO.supportsAppend()` returns `false` and `GCSOutputFile.prepare()` rejects `Strategy.APPEND` with `IllegalArgumentException`. Callers that mix CAS and APPEND in a commit log must fall back to CAS-only.
3. **ADLS APPEND**: One conditional `AppendBlock` per append; at most 50,000 appends per file between CAS compactions. CAS requires a hierarchical-namespace account (it renames) and does not work with vended SAS credentials.
4. **Large Files**: Atomic operations work best for small files (metadata, catalog state); large writes may exceed provider request timeouts.
