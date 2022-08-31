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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbMap implements AutoCloseable {

    private final static DbMap INSTANCE = new DbMap();

    public static DbMap getInstance() {
        return INSTANCE;
    }

    private final Logger _logger;

    private final DB _db;
    private final LocationSerializer _locationSerializer;
    private final KeySerializer _keySerializer;

    private DbMap() {
        _logger = LoggerFactory.getLogger(DbMap.class);
        _logger.info("Initializing mapdb");
        _db = DBMaker.memoryDirectDB().make();
        _keySerializer = new KeySerializer();
        _locationSerializer = new LocationSerializer();
    }

    public HTreeMap<PrimaryKey, RecordLocation> createMap(String name) {
        _logger.info("Create or open map for " + name);
        return _db.hashMap(name)
                .keySerializer(_keySerializer)
                .valueSerializer(_locationSerializer)
                .createOrOpen();
    }

    @Override
    public void close() throws Exception {
        _logger.info("Closing mapdb");
        _db.close();
    }
}
