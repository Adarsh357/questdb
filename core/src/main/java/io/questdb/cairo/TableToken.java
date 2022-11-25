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

public class TableToken {
    private final boolean isWal;
    private final String privateTableName;
    private final String publicTableName;
    private final int tableId;

    public TableToken(String publicTableName, String privateTableName, int tableId, boolean isWal) {
        this.publicTableName = publicTableName;
        this.privateTableName = privateTableName;
        this.tableId = tableId;
        this.isWal = isWal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableToken that = (TableToken) o;
        return tableId == that.tableId;
    }

    public String getPrivateTableName() {
        return privateTableName;
    }

    public String getPublicTableName() {
        return publicTableName;
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public int hashCode() {
        return tableId;
    }

    public boolean isWal() {
        return isWal;
    }
}