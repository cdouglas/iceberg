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
package org.apache.iceberg.benchmark.cloud.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/** Parser for workload trace files in YAML format. */
public class TraceParser {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  private TraceParser() {}

  /**
   * Parse a workload trace from a file path.
   *
   * @param path path to the YAML trace file
   * @return parsed workload configuration
   * @throws IOException if the file cannot be read or parsed
   */
  public static WorkloadConfig parse(String path) throws IOException {
    return YAML_MAPPER.readValue(new File(path), WorkloadConfig.class);
  }

  /**
   * Parse a workload trace from an input stream.
   *
   * @param stream input stream containing YAML content
   * @return parsed workload configuration
   * @throws IOException if the stream cannot be read or parsed
   */
  public static WorkloadConfig parse(InputStream stream) throws IOException {
    return YAML_MAPPER.readValue(stream, WorkloadConfig.class);
  }

  /**
   * Parse a workload trace from a classpath resource.
   *
   * @param resourcePath path to the resource (e.g., "traces/tpc-h-like.yaml")
   * @return parsed workload configuration
   * @throws IOException if the resource cannot be read or parsed
   */
  public static WorkloadConfig parseResource(String resourcePath) throws IOException {
    try (InputStream stream = TraceParser.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (stream == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      return parse(stream);
    }
  }
}
