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

import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestDbMap {

    DB _db;
    KeySerializer _keySerializer;
    LocationSerializer _locationSerializer;

    HTreeMap<PrimaryKey, RecordLocation> _map;

    @BeforeClass
    public void setup() {
        System.out.println("Initialize mapDb for testing");
        _db = DBMaker.memoryDirectDB().make();
        _keySerializer = new KeySerializer();
        _locationSerializer = new LocationSerializer();

        _map = _db.hashMap("")
                .keySerializer(_keySerializer)
                .valueSerializer(_locationSerializer)
                .createOrOpen();
    }

    @AfterClass
    public void close() {
        System.out.println("Close mapDb");
        _map.close();
        _db.close();
    }

    @Test
    public void testSerialize() {
        _map.clear();

        PrimaryKey key = new PrimaryKey(new Object[] {"111", 1, 101L, 10.1});
        RecordLocation location = this.createRecord(1);
        writeRead(_map, key, location);

        key = new PrimaryKey(new Object[] {"222", 2, 202L, 20.2});
        location = this.createRecord(1, "string_value");
        writeRead(_map, key, location);

        key = new PrimaryKey(new Object[] {"333", 3, 303L, 30.3});
        location = this.createRecord(3, 30.3);
        writeRead(_map, key, location);
    }

    @Test
    public void testAbsent() {
        _map.clear();

        PrimaryKey key = new PrimaryKey(new Object[] {1L});
        RecordLocation location = this.createRecord(1);
        writeRead(_map, key, location);

        RecordLocation nlocation = this.createRecord(3);
        Assert.assertFalse(_map.putIfAbsentBoolean(key, nlocation));

        RecordLocation rlocation = _map.get(key);
        Assert.assertEquals(location, rlocation);
    }

    @Test
    public void testReplace() {
        _map.clear();

        PrimaryKey key = new PrimaryKey(new Object[] {1L});
        RecordLocation location = this.createRecord(1);
        writeRead(_map, key, location);

        RecordLocation location2 = this.createRecord(2);
        RecordLocation location3 = this.createRecord(3);

        Assert.assertFalse(_map.replace(key, location2, location3));
        RecordLocation rlocation = _map.get(key);
        Assert.assertEquals(location, rlocation);

        Assert.assertTrue(_map.replace(key, location, location3));
        rlocation = _map.get(key);
        Assert.assertEquals(location3, rlocation);
    }

    private void writeRead(HTreeMap<PrimaryKey, RecordLocation> map, PrimaryKey key, RecordLocation value) {
        System.out.println("Write: " + value);
        map.put(key, value);
        RecordLocation rv = map.get(key);
        System.out.println(" Read: " + rv);
        Assert.assertEquals(value, rv);
    }

    private RecordLocation createRecord(int i) {
        return new RecordLocation(i, i * 11, 100L + i);
    }

    private RecordLocation createRecord(int i, Object comparisonValue) {
        return new RecordLocation(i, i * 11, comparisonValue == null ? 100L + i : comparisonValue);
    }
}
