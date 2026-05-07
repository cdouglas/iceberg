/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.google.cloud.storage;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.storage.v2.BidiWriteHandle;
import com.google.storage.v2.StorageClient;
import java.lang.reflect.Field;

public final class RapidStorageBackdoor {

  private RapidStorageBackdoor() {}

  /** The captured server-side state of a closed-but-not-finalized appendable upload. */
  public static final class CapturedState {
    public final long generation;
    public final long confirmedBytes;
    public final BidiWriteHandle writeHandle;
    public final String routingToken;

    /** The GrpcCallContext the SDK was using post-redirect — carries routing headers. */
    public final GrpcCallContext lastCallContext;

    public CapturedState(long g, long cb, BidiWriteHandle wh, String rt, GrpcCallContext ctx) {
      this.generation = g;
      this.confirmedBytes = cb;
      this.writeHandle = wh;
      this.routingToken = rt;
      this.lastCallContext = ctx;
    }

    @Override
    public String toString() {
      return "gen="
          + generation
          + " confirmedBytes="
          + confirmedBytes
          + " hasWriteHandle="
          + (writeHandle != null)
          + " writeHandleLen="
          + (writeHandle == null ? 0 : writeHandle.getHandle().size())
          + " routingToken="
          + (routingToken == null ? "<null>" : ("len=" + routingToken.length()))
          + " hasLastCallContext="
          + (lastCallContext != null);
    }
  }

  /** Returns the underlying gax StorageClient so we can issue raw bidiWriteObject RPCs. */
  public static StorageClient storageClient(Storage storage) {
    if (!(storage instanceof GrpcStorageImpl)) {
      throw new IllegalStateException("Need a gRPC Storage; got " + storage.getClass().getName());
    }
    return ((GrpcStorageImpl) storage).storageClient;
  }

  /**
   * Read the SDK's BidiUploadState after session 1 has closed its channel — this is where the
   * server's write_handle / routing_token / generation responses were stashed. Walking the chain:
   * BlobAppendableUpload -> BlobAppendableUploadImpl.delegate (AppendableSession) ->
   * ChannelSession.startFuture (ApiFuture) -> AppendableUploadState (extends BaseUploadState).
   */
  public static CapturedState capture(BlobAppendableUpload up) throws Exception {
    BlobAppendableUploadImpl impl = (BlobAppendableUploadImpl) up;

    Field delegateField = BlobAppendableUploadImpl.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    Object delegate = delegateField.get(impl);

    // The SDK wraps the AppendableSession in DecoratedWritableByteChannelSession (which holds
    // its own private 'delegate'). Peel decorators until we land on a ChannelSession subclass.
    while (!(delegate instanceof ChannelSession)) {
      Field decoratedDelegate = delegate.getClass().getDeclaredField("delegate");
      decoratedDelegate.setAccessible(true);
      delegate = decoratedDelegate.get(delegate);
    }

    Field startFutureField = ChannelSession.class.getDeclaredField("startFuture");
    startFutureField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ApiFuture<BidiUploadState.AppendableUploadState> startFuture =
        (ApiFuture<BidiUploadState.AppendableUploadState>) startFutureField.get(delegate);
    BidiUploadState.AppendableUploadState state = startFuture.get();

    // lastOpenArguments lives on BaseUploadState; carries the GrpcCallContext the SDK negotiated.
    GrpcCallContext lastCtx = null;
    Field lastOpenArgsField =
        BidiUploadState.BaseUploadState.class.getDeclaredField("lastOpenArguments");
    lastOpenArgsField.setAccessible(true);
    Object lastOpenArgs = lastOpenArgsField.get(state);
    if (lastOpenArgs != null) {
      Field ctxField = lastOpenArgs.getClass().getDeclaredField("ctx");
      ctxField.setAccessible(true);
      lastCtx = (GrpcCallContext) ctxField.get(lastOpenArgs);
    }

    return new CapturedState(
        state.generation,
        state.getConfirmedBytes(),
        state.writeHandle,
        state.routingToken,
        lastCtx);
  }
}
