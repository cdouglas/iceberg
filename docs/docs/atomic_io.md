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

    /** APPEND failed (etag/lease mismatch or wrong flush position). */
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

Implements `SupportsAtomicOperations` using GCS generation numbers.

```java
public class GCSFileIO implements DelegateFileIO, SupportsAtomicOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        GCSInputFile gcsInputFile = (GCSInputFile) replace;
        return GCSOutputFile.fromBlobId(
            gcsInputFile.blobId(), storage, gcpProperties, metrics);
    }
}
```

### GCSOutputFile

Uses `BlobWriteOption.generationMatch()` for atomic writes:

```java
class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile {
    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
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
        // Uses GCSAtomicOutputStream with generation match precondition
        try (GCSAtomicOutputStream dest = new GCSAtomicOutputStream(
                storage(), blobId(), gcpProperties(), metrics(), token, result::set)) {
            ByteStreams.copy(source.get(), dest);
        }
        return result[0];
    }
}
```

### GCS Preconditions

- **CAS**: `BlobWriteOption.generationMatch(expectedGeneration)` - fails with 412 if generation changed
- **APPEND**: Not directly supported; emulated with generation match

## S3 Implementation

### S3FileIO

Implements `SupportsAtomicOperations` using S3 ETags and conditional requests.

```java
public class S3FileIO implements CredentialSupplier, DelegateFileIO,
        SupportsAtomicOperations, SupportsRecoveryOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        S3InputFile s3Input = (S3InputFile) replace;
        return S3OutputFile.fromLocation(location, client, metrics, s3Input.etag());
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

### ADLSFileIO

Implements `SupportsAtomicOperations` using Azure DataLake ETags.

```java
public class ADLSFileIO implements DelegateFileIO, SupportsAtomicOperations {
    @Override
    public AtomicOutputFile newOutputFile(InputFile replace) {
        ADLSInputFile adlsInput = (ADLSInputFile) replace;
        DataLakeRequestConditions conditions = adlsInput.conditions();
        return new ADLSOutputFile(path, fileClient, azureProperties,
            adlsInput.getLength(), conditions, metrics);
    }
}
```

### ADLSOutputFile

Uses `DataLakeRequestConditions.setIfMatch()` for atomic writes:

```java
class ADLSOutputFile extends BaseADLSFile implements AtomicOutputFile {
    @Override
    public CAS prepare(Supplier<InputStream> source, Strategy howto) {
        ADLSChecksum checksum = new ADLSChecksum(howto);
        try (InputStream in = source.get();
             FileChecksumOutputStream chk = new FileChecksumOutputStream(
                 ByteStreams.nullOutputStream(), checksum)) {
            ByteStreams.copy(in, chk);
        }
        return checksum;
    }

    @Override
    public ADLSInputFile writeAtomic(CAS checksum, Supplier<InputStream> source) {
        ADLSChecksum token = (ADLSChecksum) checksum;
        switch (token.getStrategy()) {
            case CAS:
                return replaceDestObj(token, source);  // uploadWithResponse + conditions
            case APPEND:
                return appendDestObj(token, source);   // appendWithResponse + flushWithResponse
            default:
                throw new UnsupportedOperationException();
        }
    }
}
```

### Azure Preconditions

- **CAS**: `FileParallelUploadOptions.setRequestConditions(conditions.setIfMatch(etag))` - fails with 412
- **APPEND**: `appendWithResponse()` + `flushWithResponse()` with position validation, serialized by a blob lease (see below) - fails with 400 InvalidFlushPosition or 412 ConditionNotMet

### Azure APPEND validation gap and lease serialization

ADLS Gen2's append is a two-phase operation:

1. `appendWithResponse(stream, position=L, length=N, ...)` uploads bytes to an **uncommitted block** at file offset `L`.
2. `flushWithResponse(position=L+N, ifMatch=etag, ...)` commits the uncommitted region; the position must equal the uncommitted region's end and the etag must match the snapshot.

This pair has a validation gap that no combination of the SDK's response codes can close on its own. Concurrent same-position appends share the uncommitted buffer at offset `L`: a second append at the same position **completely replaces** both the bytes and the effective length of the prior uncommitted block. The flush then commits whatever bytes are there. With same-length concurrent payloads, both writers' flush positions match `L+N`, the etag race elects one flush winner, but the bytes at the offset are last-appender-wins **independently** of the flush winner. A `200` from `flushWithResponse` therefore tells the caller "some atomic append happened" but not "*your* bytes were committed" — empirically this anomaly fires on roughly half of contended same-length writes.

The position-flush check does prevent torn or partial writes: if the uncommitted region's end no longer matches our position, the flush fails cleanly with `400 InvalidFlushPosition` and commits nothing. So committed bytes are always *some* concurrent writer's complete payload — never a mix. But the FileIO contract demands the stronger guarantee that a successful `writeAtomic` means *this writer's* bytes are live, so `ADLSOutputFile.appendDestObj` serializes the append+flush span with a blob lease:

| Step | Call | Lease action |
|---|---|---|
| Append | `appendWithResponse(..., DataLakeFileAppendOptions.setLeaseAction(ACQUIRE).setProposedLeaseId(...).setLeaseDuration(15))` | Acquire on the same RPC as the byte upload |
| Flush | `flushWithResponse(..., DataLakeFileFlushOptions.setRequestConditions(setLeaseId(...).setIfMatch(...)).setLeaseAction(RELEASE))` | Release on the same RPC as the etag-checked commit |
| Flush failure | `finally { DataLakeLeaseClient.releaseLease() }` | Eager release so a stale-snapshot writer doesn't lock other writers out for the full duration |

The lease is piggybacked on the calls that were already happening — no extra round trips. While we hold it, no other writer can stage uncommitted data at our offset, so a `200` from flush implies our bytes are live.

**Lease timeout semantics** — uncommitted data is *not* tied to lease lifetime. If a writer crashes between append and flush:

- The lease ages out (15s) and becomes available again.
- The writer's uncommitted bytes remain orphaned at the file's offset, **invisible to readers** (`getLength()` reports only flushed bytes; streamed reads see only the committed file).
- The next writer pins the file's committed length, appends at that offset, and flushes. Their append at the same offset cleanly overwrites the orphaned uncommitted block.
- Azure's block-blob storage reclaims orphaned uncommitted data after its TTL (~7 days).

There is no scenario in which a lease timeout leads to a partial commit, a torn write, or visibility of unflushed bytes.

**Lease contention** — two simultaneous `LeaseAction.ACQUIRE` requests resolve at the *append* call: one writer acquires the lease, the other gets `409 LeaseAlreadyPresent` and is translated to `AppendException`. The loser's bytes never enter the uncommitted buffer.

### Azure Checksums

Azure uses MD5 checksums stored in content headers:

```java
class ADLSChecksum implements CAS, FileChecksum {
    private final MessageDigest md5 = MessageDigest.getInstance("MD5");

    public byte[] contentChecksumBytes() {
        return md5.digest();
    }
}
```

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
| Azure ADLS | 412 Precondition Failed → `CASException` | 412, 400 InvalidFlushPosition, or 409 LeaseAlreadyPresent → `AppendException` |

`CASException` is the base type for atomic-CAS failures; callers that need to distinguish "snapshot is stale, must reconcile" from "transient, retry the same bytes" can catch `StorageInvariantException` and `StorageThrottleException` separately. `AppendException` covers all APPEND-strategy failures.

## Limitations

1. **S3 APPEND**: Only supported on S3 Express One Zone (directory) buckets; standard buckets reject `writeOffsetBytes`. Provider tests declare this via `supportsAppend()`.
2. **GCS APPEND**: Not supported. GCS objects are immutable — every write replaces the whole object — so `GCSFileIO.supportsAppend()` returns `false` and `GCSOutputFile.prepare()` rejects `Strategy.APPEND` with `IllegalArgumentException`. Callers that mix CAS and APPEND in a commit log must fall back to CAS-only.
3. **ADLS APPEND**: Atomic via the lease mechanism described above. Concurrent writers serialize on the file's blob lease; the loser sees `AppendException` at append time, before any bytes are staged.
4. **Large Files**: Atomic operations work best for small files (metadata, catalog state); large writes may exceed provider request timeouts.
