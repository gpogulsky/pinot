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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbMap implements Closeable {

    private final static String DB_FILENAME = "upsert_map";
    private final static long ONE_MB = 1024 * 1024;
    private final static long ONE_GB = 1024 * 1024 * 1024;

    private final static DbMap INSTANCE = new DbMap();

    public static DbMap getInstance() {
        return INSTANCE;
    }

    private final Logger _logger;

    private final boolean _enabled;
    private final DB _db;
    private final LocationSerializer _locationSerializer;
    private final String _path;

    private DbMap() {
        _logger = LoggerFactory.getLogger(DbMap.class);
        _logger.info("Initializing mapdb");

        if (DbContext.getInstance().isEnabled()) {
            _logger.info("mapdb configuration is enabled. Attempting to initialize");
            _path = DbContext.getInstance().getPath() + File.separator + DB_FILENAME;
            long size = DbContext.getInstance().getSize() * ONE_MB;
            if (size == 0) {
                size = ONE_GB;
            }

            cleanPath(_path);
            _db = getMapDBDatabaseFromPath(_path, size);
        } else {
            _logger.info("mapdb configuration is disabled");
            _db = null;
            _path = null;
        }

        // Call isClosed to confirm db is alive
        if (_db != null && !_db.isClosed()) {
            _locationSerializer = new LocationSerializer();
            _enabled = true;
            _logger.info("mapdb is enabled");
        } else {
            _logger.info("mapdb is disabled");
            _enabled = false;
            _locationSerializer = null;
        }
    }

    public HTreeMap<byte[], RecordLocation> createMap(String name) {
        _logger.info("Create or open map for " + name);

        if (!_enabled) {
            throw new IllegalStateException(
                    "Usage of mapdb for location maps is either not enabled in the server configuration "
                            + "or due to initialization failure");
        }

        return _db.hashMap(name)
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(_locationSerializer)
                .counterEnable()
                .createOrOpen();
    }

    @Override
    public void close() {
        _logger.info("Closing mapdb");
        _db.close();
    }

    public void deleteFile() {
        if (!_db.isClosed()) {
            throw new IllegalStateException("Database must be closed");
        }

        this.cleanPath(_path);
    }

    private void cleanPath(String path) {
        File file = new File(path);
        if (file.exists() && file.isFile()) {
            try {
                FileUtils.delete(file);
            } catch (IOException e) {
                _logger.warn("Failed to remove file at " + path, e);
                System.out.println(e);
                e.printStackTrace();
            }
        }
    }

    private DB getMapDBDatabaseFromPath(String path, long size) {

        long increment = size < ONE_GB ? size : ONE_GB;

        DB db = null;
        try {
            db = DBMaker
                    .fileDB(path)
                    .fileMmapEnable()
                    .fileMmapEnableIfSupported()
                    .allocateStartSize(size)
                    .allocateIncrement(increment)
                    .make();
        } catch (org.mapdb.DBException.DataCorruption
                | org.mapdb.DBException.VolumeIOError e) {
            _logger.error("Failed to instantiate DB at " + path, e);
        }
        return db;
    }
}
