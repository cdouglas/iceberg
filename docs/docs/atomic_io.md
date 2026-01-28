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
     *
     * @param toReplace The input file to atomically replace
     * @return An AtomicOutputFile for conditional writes
     */
    AtomicOutputFile newOutputFile(InputFile toReplace);

    /** Thrown when a compare-and-swap operation fails due to concurrent modification */
    class CASException extends IOException { ... }

    /** Thrown when an append operation fails due to offset mismatch */
    class AppendException extends IOException { ... }
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
- **APPEND**: `appendWithResponse()` + `flushWithResponse()` with position validation - fails with 400 InvalidFlushPosition

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
| GCS | 412 Precondition Failed | 412 Precondition Failed |
| S3 | 412 PreconditionFailed, 409 ConditionalRequestConflict | InvalidWriteOffsetException |
| Azure | 412 Precondition Failed | 400 InvalidFlushPosition |

All failures are wrapped in `CASException` or `AppendException` for consistent handling.

## Limitations

1. **S3 APPEND**: Only supported on S3 Express One Zone storage class
2. **GCS APPEND**: Emulated using generation match; not true append
3. **Large Files**: Atomic operations work best for small files (metadata, catalog state); large files may timeout
4. **Eventual Consistency**: Some operations may require retries due to storage system eventual consistency
