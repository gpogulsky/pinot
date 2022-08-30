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

import java.util.Objects;

public class RecordLocation {
    private final Integer _segmentId;
    private final int _docId;
    private final Object _comparisonValue;

    public RecordLocation(Integer segmentId, int docId, Object comparisonValue) {
        _segmentId = segmentId;
        _docId = docId;
        _comparisonValue = comparisonValue;
    }

    public Integer getSegmentId() {
        return _segmentId;
    }

    public int getDocId() {
        return _docId;
    }

    public Object getComparisonValue() {
        return _comparisonValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordLocation that = (RecordLocation) o;
        return _docId == that._docId
                && _segmentId.equals(that._segmentId)
                && _comparisonValue.equals(that._comparisonValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_segmentId, _docId, _comparisonValue);
    }

    @Override
    public String toString() {
        return "RecordLocation{"
                + "_segmentId=" + _segmentId
                + ", _docId=" + _docId
                + ", _comparisonValue=" + _comparisonValue
                + '}';
    }
}
