/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.core.construction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Holds a {@link PipelineOptions} in JSON serialized form and calls {@link
 * FileSystems#setDefaultPipelineOptions(PipelineOptions)} on construction or on deserialization.
 */
public class SerializablePipelineOptions implements Serializable {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  private final String serializedPipelineOptions;
  private transient PipelineOptions options;

  public SerializablePipelineOptions(PipelineOptions options) {
    this.serializedPipelineOptions = serializeToJson(options);
    this.options = options;
    FileSystems.setDefaultPipelineOptions(options);
  }

  public PipelineOptions get() {
    return options;
  }

  private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
    is.defaultReadObject();
    this.options = deserializeFromJson(serializedPipelineOptions);
    // TODO https://issues.apache.org/jira/browse/BEAM-2712: remove this call.
    FileSystems.setDefaultPipelineOptions(options);
  }

  private static String serializeToJson(PipelineOptions options) {
    try {
      return MAPPER.writeValueAsString(options);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize PipelineOptions", e);
    }
  }

  private static PipelineOptions deserializeFromJson(String options) {
    try {
      return MAPPER.readValue(options, PipelineOptions.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to deserialize PipelineOptions", e);
    }
  }
}
