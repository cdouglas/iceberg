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
package org.apache.iceberg.data.snaprewrite;

import org.apache.iceberg.snaprewrite.SnapshotRewriteResult;
import org.junit.jupiter.api.BeforeEach;

/**
 * The lossless matrix on v3, with every rewrite taken through reclaim and reconstructed.
 *
 * <p>Each case runs unchanged: it builds its history and calls {@link #rewrite()}, which here also
 * commits the rewrite, reclaims the original layout, reconstructs it from the ledger, and checks
 * the result against the layout captured before anything was rewritten. The rewritten layout is
 * left in place rather than reclaimed, because each case goes on to read the rewrite's own result.
 */
public class TestSnapshotRewriteLosslessReconstructed extends TestSnapshotRewriteLossless {

  @BeforeEach
  public void useV3() {
    useFormatVersion(3);
  }

  @Override
  protected SnapshotRewriteResult rewrite() {
    LayoutFingerprint before = LayoutFingerprint.of(table);
    SnapshotRewriteResult result = super.rewrite();
    assertLossless(result);
    assertReconstructs(before, result);
    return result;
  }
}
