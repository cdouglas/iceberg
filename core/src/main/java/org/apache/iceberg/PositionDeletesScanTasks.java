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
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Utility class for creating {@link PositionDeletesScanTask} instances.
 *
 * <p>This class provides factory methods for creating scan tasks from delete files, which is useful
 * for actions that need to process specific delete files outside of the normal table scan flow.
 */
public class PositionDeletesScanTasks {

  private PositionDeletesScanTasks() {}

  /**
   * Creates a scan task for a position delete file.
   *
   * @param deleteFile the delete file to scan
   * @param tableSchema the table schema
   * @param spec the partition spec for the delete file
   * @return a new position deletes scan task
   */
  public static PositionDeletesScanTask create(
      DeleteFile deleteFile, Schema tableSchema, PartitionSpec spec) {
    String schemaString = SchemaParser.toJson(tableSchema);
    String specString = PartitionSpecParser.toJson(spec);
    return new BasePositionDeletesScanTask(
        deleteFile,
        schemaString,
        specString,
        ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
  }

  /**
   * Creates scan tasks for a list of position delete files.
   *
   * @param deleteFiles the delete files to scan
   * @param table the table containing the delete files
   * @return list of position deletes scan tasks
   */
  public static List<PositionDeletesScanTask> create(
      Iterable<DeleteFile> deleteFiles, Table table) {
    List<PositionDeletesScanTask> tasks = Lists.newArrayList();
    Schema tableSchema = table.schema();
    for (DeleteFile deleteFile : deleteFiles) {
      PartitionSpec spec = table.specs().get(deleteFile.specId());
      tasks.add(create(deleteFile, tableSchema, spec));
    }
    return tasks;
  }
}
