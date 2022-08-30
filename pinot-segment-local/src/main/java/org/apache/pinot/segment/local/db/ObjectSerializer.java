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


public class ObjectSerializer {

    enum ObjectType {
        Unknown(0),
        Long(1),
        String(2),
        Integer(3),
        Double(4);

        private final int _val;

        ObjectType(int i) {
            _val = i;
        }

        static ObjectType fromInt(int i) {
            switch (i) {
                case 1: return Long;
                case 2: return String;
                case 3: return Integer;
                case 4: return Double;
                default: return Unknown;
            }
        }

        int getValue() {
            return _val;
        }
    }

    public void serializeObject(DataOutput2 dataOutput2, Object value) throws IOException {

        String klass = value.getClass().getSimpleName();
        ObjectType type = Enum.valueOf(ObjectType.class, klass);

        if (type == null || type.equals(ObjectType.Unknown)) {
            throw new UnsupportedOperationException("Object serializer does not support serialization of " + klass);
        }

        dataOutput2.packInt(type.getValue());

        switch (type) {
            case Long:
                Serializer.LONG.serialize(dataOutput2, (Long) value);
                break;
            case String:
                Serializer.STRING.serialize(dataOutput2, (String) value);
                break;
            case Integer:
                Serializer.INTEGER.serialize(dataOutput2, (Integer) value);
                break;
            case Double:
                Serializer.DOUBLE.serialize(dataOutput2, (Double) value);
                break;
            default:
                throw new UnsupportedOperationException("Object serializer does not support serialization of " + klass);
        }
    }

    public Object deserializeObject(DataInput2 dataInput2, int i) throws IOException {

        int v = dataInput2.unpackInt();
        ObjectType type = ObjectType.fromInt(v);

        switch (type) {
            case Long:
                return Serializer.LONG.deserialize(dataInput2, i);
            case String:
                return Serializer.STRING.deserialize(dataInput2, i);
            case Integer:
                return Serializer.INTEGER.deserialize(dataInput2, i);
            case Double:
                return Serializer.DOUBLE.deserialize(dataInput2, i);
            default:
                // Should never happen
                throw new IOException("Unknown type " + v);
        }
    }
}
