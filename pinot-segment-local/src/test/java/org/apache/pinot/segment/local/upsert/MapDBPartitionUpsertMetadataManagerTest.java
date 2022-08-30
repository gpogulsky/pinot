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
package org.apache.pinot.segment.local.upsert;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.db.RecordLocation;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class MapDBPartitionUpsertMetadataManagerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);

  @Test
  public void testAddReplaceRemoveSegment() {
    verifyAddReplaceRemoveSegment(HashFunction.NONE);
//    verifyAddReplaceRemoveSegment(HashFunction.MD5);
//    verifyAddReplaceRemoveSegment(HashFunction.MURMUR3);
  }

  private void verifyAddReplaceRemoveSegment(HashFunction hashFunction) {
    MapDBPartitionUpsertMetadataManager upsertMetadataManager =
        new MapDBPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, Collections.singletonList("pk"),
            "timeCol", hashFunction, null, mock(ServerMetrics.class));
    //Map<Object, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;
    Map<PrimaryKey, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;
    recordLocationMap.clear();
    Map<Integer, IndexSegment> segmentMap = upsertMetadataManager._segmentIdToSegmentMap;
    segmentMap.clear();

    // Add the first segment
    int numRecords = 6;
    int[] primaryKeys = new int[]{0, 1, 2, 0, 1, 0};
    int[] timestamps = new int[]{100, 100, 100, 80, 120, 100};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    List<PrimaryKey> primaryKeys1 = getPrimaryKeyList(numRecords, primaryKeys);
    ImmutableSegmentImpl segment1 = mockImmutableSegment(1, validDocIds1, primaryKeys1);
    List<RecordInfo> recordInfoList1 = getRecordInfoList(numRecords, primaryKeys, timestamps);
    upsertMetadataManager.addSegment(segment1, validDocIds1, recordInfoList1.iterator());
    // segment1: 0 -> {5, 100}, 1 -> {4, 120}, 2 -> {2, 100}
    assertEquals(recordLocationMap.size(), 3);
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment1, 5, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment1, 2, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{2, 4, 5});

    // Add the second segment
    numRecords = 5;
    primaryKeys = new int[]{0, 1, 2, 3, 0};
    timestamps = new int[]{100, 100, 120, 80, 80};
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment2 = mockImmutableSegment(2, validDocIds2, getPrimaryKeyList(numRecords, primaryKeys));
    upsertMetadataManager.addSegment(segment2, validDocIds2,
        getRecordInfoList(numRecords, primaryKeys, timestamps).iterator());
    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment2, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 3, 80);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});

    // Add an empty segment
    EmptyIndexSegment emptySegment = mockEmptySegment(3);
    upsertMetadataManager.addSegment(emptySegment);
    // segment1: 1 -> {4, 120}
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment2, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 4, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 3, 80);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});

    // Replace (reload) the first segment
    ThreadSafeMutableRoaringBitmap newValidDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl newSegment1 = mockImmutableSegment(1, newValidDocIds1, primaryKeys1);
    upsertMetadataManager.replaceSegment(newSegment1, newValidDocIds1, recordInfoList1.iterator(), segment1);
    // original segment1: 1 -> {4, 120} (not in the map)
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment2, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, newSegment1, 4, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 3, 80);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(upsertMetadataManager._replacedSegments, Collections.singleton(segment1));

    // Remove the original segment1
    upsertMetadataManager.removeSegment(segment1);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment2, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, newSegment1, 4, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 3, 80);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
    assertTrue(upsertMetadataManager._replacedSegments.isEmpty());

    // Remove the empty segment
    upsertMetadataManager.removeSegment(emptySegment);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80}
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 4);
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment2, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, newSegment1, 4, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 2, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 3, 80);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});

    // Remove segment2
    upsertMetadataManager.removeSegment(segment2);
    // segment2: 0 -> {0, 100}, 2 -> {2, 120}, 3 -> {3, 80} (not in the map)
    // new segment1: 1 -> {4, 120}
    assertEquals(recordLocationMap.size(), 1);
    checkRecordLocation(recordLocationMap, segmentMap, 1, newSegment1, 4, 120);
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 2, 3});
    assertEquals(newValidDocIds1.getMutableRoaringBitmap().toArray(), new int[]{4});
  }

  private List<RecordInfo> getRecordInfoList(int numRecords, int[] primaryKeys, int[] timestamps) {
    List<RecordInfo> recordInfoList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      recordInfoList.add(new RecordInfo(makePrimaryKey(primaryKeys[i]), i, timestamps[i]));
    }
    return recordInfoList;
  }

  private List<PrimaryKey> getPrimaryKeyList(int numRecords, int[] primaryKeys) {
    List<PrimaryKey> primaryKeyList = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      primaryKeyList.add(makePrimaryKey(primaryKeys[i]));
    }
    return primaryKeyList;
  }

  private static ImmutableSegmentImpl mockImmutableSegment(int sequenceNumber,
      ThreadSafeMutableRoaringBitmap validDocIds, List<PrimaryKey> primaryKeys) {
    ImmutableSegmentImpl segment = mock(ImmutableSegmentImpl.class);
    when(segment.getSegmentName()).thenReturn(getSegmentName(sequenceNumber));
    when(segment.getValidDocIds()).thenReturn(validDocIds);
    when(segment.getValue(anyInt(), anyString())).thenAnswer(
        invocation -> primaryKeys.get(invocation.getArgument(0)).getValues()[0]);
    return segment;
  }

  private static EmptyIndexSegment mockEmptySegment(int sequenceNumber) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn(getSegmentName(sequenceNumber));
    return new EmptyIndexSegment(segmentMetadata);
  }

  private static MutableSegment mockMutableSegment(int sequenceNumber, ThreadSafeMutableRoaringBitmap validDocIds) {
    MutableSegment segment = mock(MutableSegment.class);
    when(segment.getSegmentName()).thenReturn(getSegmentName(sequenceNumber));
    when(segment.getValidDocIds()).thenReturn(validDocIds);
    return segment;
  }

  private static String getSegmentName(int sequenceNumber) {
    return new LLCSegmentName(RAW_TABLE_NAME, 0, sequenceNumber, System.currentTimeMillis()).toString();
  }

  private static PrimaryKey makePrimaryKey(int value) {
    return new PrimaryKey(new Object[]{value});
  }

  private static void checkRecordLocation(Map<PrimaryKey, RecordLocation> recordLocationMap,
          Map<Integer, IndexSegment> segmentMap, int keyValue, IndexSegment segment, int docId, int comparisonValue) {

    RecordLocation recordLocation = recordLocationMap.get(makePrimaryKey(keyValue));
    IndexSegment storedSegment = segmentMap.get(recordLocation.getSegmentId());

    assertNotNull(recordLocation);
    assertNotNull(storedSegment);
    assertSame(storedSegment, segment);
    assertEquals(recordLocation.getDocId(), docId);
    assertEquals(recordLocation.getComparisonValue(), comparisonValue);
  }


  @Test
  public void testAddRecord() {
    verifyAddRecord(HashFunction.NONE);
//    verifyAddRecord(HashFunction.MD5);
//    verifyAddRecord(HashFunction.MURMUR3);
  }

  private void verifyAddRecord(HashFunction hashFunction) {
    MapDBPartitionUpsertMetadataManager upsertMetadataManager =
        new MapDBPartitionUpsertMetadataManager(REALTIME_TABLE_NAME, 0, Collections.singletonList("pk"),
            "timeCol", hashFunction, null, mock(ServerMetrics.class));
    //Map<Object, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;
    Map<PrimaryKey, RecordLocation> recordLocationMap = upsertMetadataManager._primaryKeyToRecordLocationMap;
    recordLocationMap.clear();
    Map<Integer, IndexSegment> segmentMap = upsertMetadataManager._segmentIdToSegmentMap;
    segmentMap.clear();

    // Add the first segment
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    int numRecords = 3;
    int[] primaryKeys = new int[]{0, 1, 2};
    int[] timestamps = new int[]{100, 120, 100};
    ThreadSafeMutableRoaringBitmap validDocIds1 = new ThreadSafeMutableRoaringBitmap();
    ImmutableSegmentImpl segment1 = mockImmutableSegment(1, validDocIds1, getPrimaryKeyList(numRecords, primaryKeys));
    upsertMetadataManager.addSegment(segment1, validDocIds1,
        getRecordInfoList(numRecords, primaryKeys, timestamps).iterator());

    // Update records from the second segment
    ThreadSafeMutableRoaringBitmap validDocIds2 = new ThreadSafeMutableRoaringBitmap();
    MutableSegment segment2 = mockMutableSegment(2, validDocIds2);
    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(3), 0, 100));

    // segment1: 0 -> {0, 100}, 1 -> {1, 120}, 2 -> {2, 100}
    // segment2: 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment1, 2, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 2});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0});

    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(2), 1, 120));
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});

    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(1), 2, 100));
    // segment1: 0 -> {0, 100}, 1 -> {1, 120}
    // segment2: 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment1, 0, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{0, 1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1});

    upsertMetadataManager.addRecord(segment2, new RecordInfo(makePrimaryKey(0), 3, 100));
    // segment1: 1 -> {1, 120}
    // segment2: 0 -> {3, 100}, 2 -> {1, 120}, 3 -> {0, 100}
    checkRecordLocation(recordLocationMap, segmentMap, 0, segment2, 3, 100);
    checkRecordLocation(recordLocationMap, segmentMap, 1, segment1, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 2, segment2, 1, 120);
    checkRecordLocation(recordLocationMap, segmentMap, 3, segment2, 0, 100);
    assertEquals(validDocIds1.getMutableRoaringBitmap().toArray(), new int[]{1});
    assertEquals(validDocIds2.getMutableRoaringBitmap().toArray(), new int[]{0, 1, 3});
  }

  //@Test
  public void testUpsertConfig() throws JsonProcessingException {

    String upsertConfigString = "{\n"
            + "    \"mode\": \"FULL\",\n"
            + "    \"comparisonColumn\": \"anotherTimeColumn\",\n"
            + "    \"hashFunction\": \"NONE\",\n"
            + "    \"metadataManagerClass\": \"SuperSpecialManagerClass\"\n"
            + "  }";

    UpsertConfig upsertConfig = JsonUtils.stringToObject(upsertConfigString, UpsertConfig.class);

    System.out.println(upsertConfig);
  }
}
