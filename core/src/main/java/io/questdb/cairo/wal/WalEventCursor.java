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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;

import static io.questdb.cairo.wal.WalTxnType.*;

public class WalEventCursor {
    public static final long END_OF_EVENTS = -1L;

    private final DataInfo dataInfo = new DataInfo();
    private final SqlInfo sqlInfo = new SqlInfo();

    private final MemoryMR eventMem;
    private long memSize;
    private long offset = Integer.BYTES; // skip wal meta version
    private long txn = END_OF_EVENTS;
    private byte type = NONE;
    private long nextOffset = Integer.BYTES;

    public WalEventCursor(MemoryMR eventMem) {
        this.eventMem = eventMem;
    }

    public void reset() {
        memSize = eventMem.size();
        nextOffset = Integer.BYTES; // skip wal meta version
        txn = END_OF_EVENTS;
        type = WalTxnType.NONE;
    }

    public boolean setPosition(long segmentTxn) {
        reset();

        if (segmentTxn > -1) {
            while (true) {
                offset = nextOffset;
                int length = readInt();
                if (length < 1) {
                    // EOF
                    return false;
                }
                nextOffset = length + nextOffset;
                txn = readLong();

                if (txn == segmentTxn) {
                    readRecord();
                    return true;
                } else if (txn == END_OF_EVENTS) {
                    return false;
                }
            }
        }

        // Read all from beginning
        return true;
    }

    public boolean hasNext() {
        offset = nextOffset;
        int length = readInt();
        if (length < 1) {
            // EOF
            return false;
        }
        nextOffset = length + nextOffset;

        txn = readLong();
        if (txn == END_OF_EVENTS) {
            return false;
        }
        readRecord();
        return true;
    }

    private void readRecord() {
        type = readByte();
        switch (type) {
            case DATA:
                dataInfo.read();
                break;
            case SQL:
                sqlInfo.read();
                break;
            default:
                throw CairoException.critical(CairoException.METADATA_VALIDATION).put("Unsupported WAL event type: ").put(type);
        }
    }

    public DataInfo getDataInfo() {
        if (type != DATA) {
            throw CairoException.critical(CairoException.ILLEGAL_OPERATION).put("WAL event type is not DATA, type=").put(type);
        }
        return dataInfo;
    }

    public SqlInfo getSqlInfo() {
        if (type != SQL) {
            throw CairoException.critical(CairoException.ILLEGAL_OPERATION).put("WAL event type is not SQL, type=").put(type);
        }
        return sqlInfo;
    }

    public long getTxn() {
        return txn;
    }

    public byte getType() {
        return type;
    }

    public class DataInfo implements SymbolMapDiffCursor {
        private final SymbolMapDiffImpl symbolMapDiff = new SymbolMapDiffImpl(WalEventCursor.this);
        private long startRowID;
        private long endRowID;
        private long minTimestamp;
        private long maxTimestamp;
        private boolean outOfOrder;

        private void read() {
            startRowID = readLong();
            endRowID = readLong();
            minTimestamp = readLong();
            maxTimestamp = readLong();
            outOfOrder = readBool();
        }

        public long getStartRowID() {
            return startRowID;
        }

        public long getEndRowID() {
            return endRowID;
        }

        public long getMinTimestamp() {
            return minTimestamp;
        }

        public long getMaxTimestamp() {
            return maxTimestamp;
        }

        public boolean isOutOfOrder() {
            return outOfOrder;
        }

        public SymbolMapDiff nextSymbolMapDiff() {
            return readNextSymbolMapDiff(symbolMapDiff);
        }
    }

    public class SqlInfo {
        private int cmdType;
        private CharSequence sql;

        private void read() {
            cmdType = readInt();
            sql = readStr();
        }

        public int getCmdType() {
            return cmdType;
        }

        public CharSequence getSql() {
            return sql;
        }
    }

    private long readLong() {
        checkMemSize(Long.BYTES);
        final long value = eventMem.getLong(offset);
        offset += Long.BYTES;
        return value;
    }

    private int readInt() {
        checkMemSize(Integer.BYTES);
        final int value = eventMem.getInt(offset);
        offset += Integer.BYTES;
        return value;
    }

    private byte readByte() {
        checkMemSize(Byte.BYTES);
        final byte value = eventMem.getByte(offset);
        offset += Byte.BYTES;
        return value;
    }

    private boolean readBool() {
        checkMemSize(Byte.BYTES);
        final boolean value = eventMem.getBool(offset);
        offset += Byte.BYTES;
        return value;
    }

    private CharSequence readStr() {
        checkMemSize(Integer.BYTES);
        final int strLength = eventMem.getInt(offset);
        final long storageLength = Vm.getStorageLength(strLength);

        checkMemSize(storageLength);
        final CharSequence value = eventMem.getStr(offset);
        offset += storageLength;
        return value;
    }

    SymbolMapDiff readNextSymbolMapDiff(SymbolMapDiffImpl symbolMapDiff) {
        final int columnIndex = readInt();
        if (columnIndex == SymbolMapDiffImpl.END_OF_SYMBOL_DIFFS) {
            return null;
        }
        final int cleanTableSymbolCount = readInt();
        final int size = readInt();

        symbolMapDiff.of(columnIndex, cleanTableSymbolCount, size);
        return symbolMapDiff;
    }

    SymbolMapDiffImpl.Entry readNextSymbolMapDiffEntry(SymbolMapDiffImpl.Entry entry) {
        final int key = readInt();
        if (key == SymbolMapDiffImpl.END_OF_SYMBOL_ENTRIES) {
            entry.clear();
            return null;
        }
        final CharSequence symbol = readStr();
        entry.of(key, symbol);
        return entry;
    }

    private void checkMemSize(long requiredBytes) {
        if (memSize < offset + requiredBytes) {
            throw CairoException.critical(0).put("WAL event file is too small, size=").put(memSize)
                    .put(", required=").put(offset + requiredBytes);
        }
    }
}
