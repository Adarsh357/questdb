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

package io.questdb.griffin.wal.fuzz;

import io.questdb.cairo.TestRecord;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;

public class FuzzAddColumnOperation implements FuzzTransactionOperation {

    private final String newColName;
    private final int newType;
    private final boolean indexFlag;
    private final int indexValueBlockCapacity;
    private final boolean symbolTableStatic;

    public FuzzAddColumnOperation(RecordMetadata tableModel, String newColName, int newType, boolean indexFlag, int indexValueBlockCapacity, boolean symbolTableStatic) {

        this.newColName = newColName;
        this.newType = newType;
        this.indexFlag = indexFlag;
        this.indexValueBlockCapacity = indexValueBlockCapacity;
        this.symbolTableStatic = symbolTableStatic;
    }

    @Override
    public boolean apply(Rnd tempRnd, TableWriterAPI tableWriter, String tableName, int tableId, IntList tempList, TestRecord.ArrayBinarySequence tempBinarySequence) {
        try {
            AlterOperationBuilder builder = new AlterOperationBuilder().ofAddColumn(0, tableName, tableId);
            builder.ofAddColumn(newColName, newType, 256, symbolTableStatic, indexFlag, indexValueBlockCapacity);
            AlterOperation alter = builder.build();
            tableWriter.apply(alter, true);
            return true;
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }
}