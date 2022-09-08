/**
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
package org.apache.pinot.plugin.ingestion.batch.standalone;

import java.util.ArrayList;
import java.util.Map;
import org.apache.pinot.plugin.ingestion.batch.common.BaseSegmentPushJobRunner;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


public class SegmentUriPushJobRunner extends BaseSegmentPushJobRunner {

  public SegmentUriPushJobRunner() {
  }

  public SegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  public void uploadSegments(Map<String, String> segmentsUriToTarPathMap)
      throws AttemptsExceededException, RetriableOperationException {
    SegmentPushUtils.sendSegmentUris(_spec, new ArrayList<>(segmentsUriToTarPathMap.keySet()));
  }
}
