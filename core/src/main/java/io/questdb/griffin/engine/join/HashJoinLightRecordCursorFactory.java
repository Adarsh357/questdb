/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.join;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Transient;

public class HashJoinLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory masterFactory;
    private final RecordCursorFactory slaveFactory;
    private final RecordSink masterKeySink;
    private final RecordSink slaveKeySink;
    private final HashJoinRecordCursor cursor;

    public HashJoinLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory masterFactory,
            RecordCursorFactory slaveFactory,
            @Transient ColumnTypes joinColumnTypes,
            @Transient ColumnTypes valueTypes, // this expected to be just LONG, we store chain references in map
            RecordSink masterKeySink,
            RecordSink slaveKeySink,
            int columnSplit
    ) {
        super(metadata);
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
        this.cursor = new HashJoinRecordCursor(columnSplit, configuration, joinColumnTypes, valueTypes);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Hash join light");
        sink.child(masterFactory);
        sink.child("Hash", slaveFactory);
    }

    @Override
    protected void _close() {
        ((JoinRecordMetadata) getMetadata()).close();
        masterFactory.close();
        slaveFactory.close();
        cursor.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor slaveCursor = slaveFactory.getCursor(executionContext);
        RecordCursor masterCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            this.cursor.of(masterCursor, slaveCursor, executionContext.getCircuitBreaker());
            return this.cursor;
        } catch (Throwable e) {
            Misc.free(slaveCursor);
            Misc.free(masterCursor);
            throw e;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean hasDescendingOrder() {
        return masterFactory.hasDescendingOrder();
    }

    @Override
    public boolean supportsUpdateRowId(CharSequence tableName) {
        return masterFactory.supportsUpdateRowId(tableName);
    }

    private class HashJoinRecordCursor extends AbstractJoinCursor {
        private final JoinRecord record;
        private final LongChain slaveChain;
        private final Map joinKeyMap;
        private Record masterRecord;
        private LongChain.TreeCursor slaveChainCursor;
        private Record slaveRecord;
        private boolean isOpen;

        public HashJoinRecordCursor(int columnSplit, CairoConfiguration configuration, ColumnTypes joinColumnTypes, ColumnTypes valueTypes) {
            super(columnSplit);
            this.record = new JoinRecord(columnSplit);
            this.joinKeyMap = MapFactory.createMap(configuration, joinColumnTypes, valueTypes);
            this.slaveChain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages());
            this.isOpen = true;
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            slaveChainCursor = null;
        }

        @Override
        public boolean hasNext() {
            if (slaveChainCursor != null && slaveChainCursor.hasNext()) {
                slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                return true;
            }

            while (masterCursor.hasNext()) {
                MapKey key = joinKeyMap.withKey();
                key.put(masterRecord, masterKeySink);
                MapValue value = key.findValue();
                if (value != null) {
                    slaveChainCursor = slaveChain.getCursor(value.getLong(0));
                    // we know cursor has values
                    // advance to get first value
                    slaveChainCursor.hasNext();
                    slaveCursor.recordAt(slaveRecord, slaveChainCursor.next());
                    return true;
                }
            }
            return false;
        }

        void of(RecordCursor masterCursor, RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            try {
                if (!isOpen) {
                    this.isOpen = true;
                    this.joinKeyMap.reopen();
                    this.slaveChain.reopen();
                }

                buildMapOfSlaveRecords(slaveCursor, circuitBreaker);

                this.masterCursor = masterCursor;
                this.slaveCursor = slaveCursor;
                this.masterRecord = masterCursor.getRecord();
                this.slaveRecord = slaveCursor.getRecordB();
                record.of(masterRecord, slaveRecord);
                slaveChainCursor = null;
            } catch (Throwable t) {
                close();
                throw t;
            }
        }

        private void buildMapOfSlaveRecords(RecordCursor slaveCursor, SqlExecutionCircuitBreaker circuitBreaker) {
            final Record record = slaveCursor.getRecord();
            while (slaveCursor.hasNext()) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                MapKey key = joinKeyMap.withKey();
                key.put(record, slaveKeySink);
                MapValue value = key.createValue();
                if (value.isNew()) {
                    final long offset = slaveChain.put(record.getRowId(), -1);
                    value.putLong(0, offset);
                    value.putLong(1, offset);
                } else {
                    value.putLong(1, slaveChain.put(record.getRowId(), value.getLong(1)));
                }
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                joinKeyMap.close();
                slaveChain.close();
                super.close();
            }
        }
    }
}
