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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableNameRegistry;
import io.questdb.cairo.TableToken;
import io.questdb.std.Misc;

import java.util.Map;

public abstract class AbstractTableNameRegistry implements TableNameRegistry {
    protected static final String TABLE_DROPPED_MARKER = "TABLE_DROPPED_MARKER:..";
    protected final TableNameRegistryFileStore tableNameStore;
    private Map<TableToken, String> reverseTableNameCache;
    private Map<CharSequence, TableToken> systemTableNameCache;

    public AbstractTableNameRegistry(CairoConfiguration configuration) {
        this.tableNameStore = new TableNameRegistryFileStore(configuration);
    }

    @Override
    public void close() {
        systemTableNameCache.clear();
        reverseTableNameCache.clear();
        Misc.free(tableNameStore);
    }

    @Override
    public TableToken getSystemName(CharSequence tableName) {
        return systemTableNameCache.get(tableName);
    }

    @Override
    public String getTableNameBySystemName(TableToken systemTableName) {
        String tableName = reverseTableNameCache.get(systemTableName);

        //noinspection StringEquality
        if (tableName == TABLE_DROPPED_MARKER) {
            return null;
        }

        return tableName;
    }

    @Override
    public Iterable<TableToken> getTableSystemNames() {
        return reverseTableNameCache.keySet();
    }

    @Override
    public boolean isWalTableDropped(TableToken systemTableName) {
        //noinspection StringEquality
        return reverseTableNameCache.get(systemTableName) == TABLE_DROPPED_MARKER;
    }

    @Override
    public boolean isWalTableName(CharSequence tableName) {
        TableToken nameRecord = systemTableNameCache.get(tableName);
        return nameRecord != null && nameRecord.isWal();
    }

    @Override
    public void resetMemory() {
        tableNameStore.resetMemory();
    }

    public void setNameMaps(
            Map<CharSequence, TableToken> systemTableNameCache,
            Map<TableToken, String> reverseTableNameCache) {
        this.systemTableNameCache = systemTableNameCache;
        this.reverseTableNameCache = reverseTableNameCache;
    }
}
