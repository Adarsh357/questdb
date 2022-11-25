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

import io.questdb.cairo.wal.AbstractTableNameRegistry;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;

import java.util.concurrent.ConcurrentHashMap;

public class TableNameRegistryRW extends AbstractTableNameRegistry {
    private static final Log LOG = LogFactory.getLog(TableNameRegistryRW.class);
    private final ConcurrentHashMap<TableToken, String> reverseTableNameCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CharSequence, TableToken> systemTableNameCache = new ConcurrentHashMap<>();

    public TableNameRegistryRW(CairoConfiguration configuration) {
        super(configuration);
        if (!this.tableNameStore.lock()) {
            if (!configuration.getAllowTableRegistrySharedWrite()) {
                throw CairoException.critical(0).put("cannot lock table name registry file [path=").put(configuration.getRoot()).put(']');
            }
        }
        setNameMaps(systemTableNameCache, reverseTableNameCache);
    }

    @Override
    public TableToken registerName(String tableName, String systemTableName, int tableId, boolean isWal) {
        TableToken newNameRecord = new TableToken(tableName, systemTableName, tableId, isWal);
        TableToken registeredRecord = systemTableNameCache.putIfAbsent(tableName, newNameRecord);

        if (registeredRecord == null) {
            if (isWal) {
                tableNameStore.appendEntry(tableName, newNameRecord);
            }
            reverseTableNameCache.put(newNameRecord, tableName);
            return newNameRecord;
        } else {
            return null;
        }
    }

    @Override
    public synchronized void reloadTableNameCache() {
        LOG.info().$("reloading table to system name mappings").$();
        systemTableNameCache.clear();
        reverseTableNameCache.clear();
        tableNameStore.reload(systemTableNameCache, reverseTableNameCache, TABLE_DROPPED_MARKER);
    }

    @Override
    public boolean removeTableName(CharSequence tableName, TableToken systemTableName) {
        TableToken nameRecord = systemTableNameCache.get(tableName);
        if (nameRecord != null
                && nameRecord.equals(systemTableName)
                && systemTableNameCache.remove(tableName, nameRecord)) {
            reverseTableNameCache.remove(nameRecord);
            return true;
        }
        return false;
    }

    @Override
    public void removeTableSystemName(TableToken systemTableName) {
        reverseTableNameCache.remove(systemTableName);
    }

    @Override
    public boolean removeWalTableName(CharSequence tableName, TableToken systemTableName) {
        TableToken nameRecord = systemTableNameCache.get(tableName);
        if (nameRecord != null
                && nameRecord.equals(systemTableName)
                && systemTableNameCache.remove(tableName, nameRecord)) {
            assert nameRecord.isWal();
            tableNameStore.removeEntry(tableName, systemTableName);
            reverseTableNameCache.put(systemTableName, TABLE_DROPPED_MARKER);
            return true;
        }
        return false;
    }

    @Override
    public String rename(CharSequence oldName, CharSequence newName, TableToken systemTableName) {
        TableToken tableRecord = systemTableNameCache.get(oldName);
        String newTableNameStr = Chars.toString(newName);

        if (systemTableNameCache.putIfAbsent(newTableNameStr, tableRecord) == null) {
            tableNameStore.removeEntry(oldName, systemTableName);
            tableNameStore.appendEntry(newName, systemTableName);
            reverseTableNameCache.put(systemTableName, newTableNameStr);
            systemTableNameCache.remove(oldName, tableRecord);
            return newTableNameStr;
        } else {
            throw CairoException.nonCritical().put("table '").put(newName).put("' already exists");
        }
    }

    @Override
    public void resetMemory() {
        tableNameStore.resetMemory();
    }
}
