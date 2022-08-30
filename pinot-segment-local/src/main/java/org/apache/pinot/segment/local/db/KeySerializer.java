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
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;


public class KeySerializer extends ObjectSerializer implements Serializer<PrimaryKey> {

    @Override
    public void serialize(DataOutput2 dataOutput2, PrimaryKey value) throws IOException {

        Object[] values = value.getValues();
        dataOutput2.packInt(values.length);
        for (Object obj : values) {
            this.serializeObject(dataOutput2, obj);
        }
    }

    @Override
    public PrimaryKey deserialize(DataInput2 dataInput2, int i) throws IOException {

        int len = dataInput2.unpackInt();
        Object[] values = new Object[len];
        for (int j = 0; j < len; j++) {
            values[j] = this.deserializeObject(dataInput2, j);
        }

        return new PrimaryKey(values);
    }
}
