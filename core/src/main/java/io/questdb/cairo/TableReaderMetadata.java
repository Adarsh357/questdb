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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

public class TableReaderMetadata extends BaseRecordMetadata implements TableRecordMetadata {
    private Path path;
    private final FilesFacade ff;
    private final LowerCaseCharSequenceIntHashMap tmpValidationMap = new LowerCaseCharSequenceIntHashMap();
    private String tableName;
    private MemoryMR metaMem;
    private int partitionBy;
    private int tableId;
    private int maxUncommittedRows;
    private long commitLag;
    private long structureVersion;
    private MemoryMR transitionMeta;
    private boolean walEnabled;

    public TableReaderMetadata(FilesFacade ff, String tableName) {
        this.path = new Path();
        this.tableName = tableName;
        this.ff = ff;
        this.metaMem = Vm.getMRInstance();
        this.columnMetadata = new ObjList<>(columnCount);
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    public TableReaderMetadata(FilesFacade ff, String tableName, Path path) {
        this(ff, tableName);
        try {
            deferredInit(path, tableName, ColumnType.VERSION);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        // TableReaderMetadata is re-usable after close, don't assign nulls
        Misc.free(metaMem);
        path = Misc.free(path);
        Misc.free(transitionMeta);
    }

    public void applyTransitionIndex() {
        // swap meta and transitionMeta
        MemoryMR temp = this.metaMem;
        this.metaMem = this.transitionMeta;
        transitionMeta = temp;
        transitionMeta.close(); // Memory is safe to double close, do not assign null to transitionMeta
        this.columnNameIndexMap.clear();
        int existingColumnCount = this.columnCount;

        int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
        assert columnCount >= existingColumnCount;
        columnMetadata.setPos(columnCount);
        int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        this.tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
        this.structureVersion = metaMem.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION);
        this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
        this.commitLag = metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
        this.walEnabled = metaMem.getInt(TableUtils.META_OFFSET_WAL_ENABLED) > 0;
        long offset = TableUtils.getColumnNameOffset(columnCount);

        int shiftLeft = 0, existingIndex = 0;
        for (int metaIndex = 0; metaIndex < columnCount; metaIndex++) {
            CharSequence name = metaMem.getStr(offset);
            offset += Vm.getStorageLength(name);
            assert name != null;
            int columnType = TableUtils.getColumnType(metaMem, metaIndex);
            boolean isIndexed = TableUtils.isColumnIndexed(metaMem, metaIndex);
            int indexBlockCapacity = TableUtils.getIndexBlockCapacity(metaMem, metaIndex);
            TableColumnMetadata existing = null;
            String newName;

            if (existingIndex < existingColumnCount) {
                existing = columnMetadata.getQuick(existingIndex);

                if (existing.getWriterIndex() > metaIndex) {
                    // This column must be deleted so existing dense columns do not contain it
                    assert columnType < 0;
                    continue;
                }
            }
            assert existing == null || existing.getWriterIndex() == metaIndex; // Same column

            // exiting column
            if (columnType < 0) {
                shiftLeft++;
            } else {
                boolean rename = existing != null && !Chars.equals(existing.getName(), name);
                newName = rename || existing == null ? Chars.toString(name) : existing.getName();
                if (rename
                        || existing == null
                        || existing.isIndexed() != isIndexed
                        || existing.getIndexValueBlockCapacity() != indexBlockCapacity
                ) {
                    columnMetadata.setQuick(existingIndex - shiftLeft,
                            new TableColumnMetadata(
                                    newName,
                                    TableUtils.getColumnHash(metaMem, metaIndex),
                                    columnType,
                                    isIndexed,
                                    indexBlockCapacity,
                                    true,
                                    null,
                                    metaIndex
                            )
                    );
                } else if (shiftLeft > 0) {
                    columnMetadata.setQuick(existingIndex - shiftLeft, existing);
                }
                this.columnNameIndexMap.put(newName, existingIndex - shiftLeft);
                if (timestampIndex == metaIndex) {
                    this.timestampIndex = existingIndex - shiftLeft;
                }
            }
            existingIndex++;
        }
        columnMetadata.setPos(existingIndex - shiftLeft);
        this.columnCount = columnMetadata.size();
        if (timestampIndex < 0) {
            this.timestampIndex = timestampIndex;
        }
    }

    public void clear() {
        Misc.free(metaMem);
    }

    public void deferredInit(Path path, String tableName, int expectedVersion) {
        this.path.of(path).$();
        deferredInit(tableName, expectedVersion);
    }

    public long createTransitionIndex(long txnStructureVersion) {
        if (transitionMeta == null) {
            transitionMeta = Vm.getMRInstance();
        }

        transitionMeta.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
        if (transitionMeta.size() >= TableUtils.META_OFFSET_STRUCTURE_VERSION + 8
                && txnStructureVersion != transitionMeta.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION)) {
            // No match
            return -1;
        }

        tmpValidationMap.clear();
        TableUtils.validate(transitionMeta, tmpValidationMap, ColumnType.VERSION);
        return TableUtils.createTransitionIndex(transitionMeta, this);
    }

    public void dumpTo(MemoryMA mem) {
        // Since _meta files are immutable and get updated with a single atomic rename
        // operation replacing the old file with the new one, it's ok to clone the metadata
        // by copying metaMem's contents. Even if _meta file was already replaced, the file
        // should be still kept on disk until inode's ref counter is above zero.
        long len = metaMem.size();
        for (long p = 0; p < len; p++) {
            mem.putByte(metaMem.getByte(p));
        }
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    public long getCommitLag() {
        return commitLag;
    }

    public int getId() {
        return tableId;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public void readSafe(CharSequence dbRoot, String tableName, MillisecondClock millisecondClock, long timeout) {
        long deadline = millisecondClock.getTicks() + timeout;
        this.path.of(dbRoot).concat(tableName);
        int rootLen = this.path.length();
        this.path.concat(TableUtils.META_FILE_NAME).$();
        boolean existenceChecked = false;
        while (true) {
            try {
                deferredInit(tableName, ColumnType.VERSION);
                return;
            } catch (CairoException ex) {
                if (!existenceChecked) {
                    path.trimTo(rootLen).slash$();
                    if (!ff.exists(path)) {
                        throw CairoException.instance(2).put("table does not exist [table=").put(tableName).put(']');
                    }
                    path.trimTo(rootLen).concat(TableUtils.META_FILE_NAME).$();
                }
                existenceChecked = true;
                TableUtils.handleMetadataLoadException(tableName, deadline, ex, millisecondClock, timeout);
            }
        }
    }

    private void deferredInit(String tableName, int expectedVersion) {
        try {
            this.tableName = tableName;
            this.metaMem.smallFile(ff, this.path, MemoryTag.MMAP_DEFAULT);
            this.columnNameIndexMap.clear();
            TableUtils.validate(metaMem, this.columnNameIndexMap, expectedVersion);
            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            this.partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
            this.tableId = metaMem.getInt(TableUtils.META_OFFSET_TABLE_ID);
            this.maxUncommittedRows = metaMem.getInt(TableUtils.META_OFFSET_MAX_UNCOMMITTED_ROWS);
            this.commitLag = metaMem.getLong(TableUtils.META_OFFSET_COMMIT_LAG);
            this.structureVersion = metaMem.getLong(TableUtils.META_OFFSET_STRUCTURE_VERSION);
            this.walEnabled = metaMem.getInt(TableUtils.META_OFFSET_WAL_ENABLED) > 0;
            this.columnMetadata.clear();
            long offset = TableUtils.getColumnNameOffset(columnCount);
            this.timestampIndex = -1;

            // don't create strings in this loop, we already have them in columnNameIndexMap
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                assert name != null;
                int columnType = TableUtils.getColumnType(metaMem, i);
                if (columnType > 0) {
                    columnMetadata.add(
                            new TableColumnMetadata(
                                    Chars.toString(name),
                                    TableUtils.getColumnHash(metaMem, i),
                                    columnType,
                                    TableUtils.isColumnIndexed(metaMem, i),
                                    TableUtils.getIndexBlockCapacity(metaMem, i),
                                    true,
                                    null,
                                    i
                            )
                    );
                    if (i == timestampIndex) {
                        this.timestampIndex = columnMetadata.size() - 1;
                    }
                }
                offset += Vm.getStorageLength(name);
            }
            this.columnCount = columnMetadata.size();
        } catch (Throwable e) {
            clear();
            throw e;
        }
    }

    public boolean isWalEnabled() {
        return walEnabled;
    }
}
