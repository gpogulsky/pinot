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
package org.apache.pinot.segment.local.db;

import java.io.IOException;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;


public class LocationSerializer extends ObjectSerializer implements Serializer<RecordLocation> {

    @Override
    public void serialize(DataOutput2 dataOutput2, RecordLocation value) throws IOException {
        Serializer.INTEGER.serialize(dataOutput2, value.getSegmentId());
        Serializer.INTEGER.serialize(dataOutput2, value.getDocId());
        this.serializeObject(dataOutput2, value.getComparisonValue());
    }

    @Override
    public RecordLocation deserialize(DataInput2 dataInput2, int i) throws IOException {
        Integer segmentId = Serializer.INTEGER.deserialize(dataInput2, i);
        int docId = Serializer.INTEGER.deserialize(dataInput2, i);
        Object comparisonValue = this.deserializeObject(dataInput2, i);

        return new RecordLocation(segmentId, docId, comparisonValue);
    }
}
