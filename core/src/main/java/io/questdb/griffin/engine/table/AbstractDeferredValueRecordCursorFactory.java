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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRandomRecordCursor;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.std.IntList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class AbstractDeferredValueRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {

    protected final Function filter;
    protected final int columnIndex;
    protected final Function symbolFunc;
    private AbstractDataFrameRecordCursor cursor;
    protected final IntList columnIndexes;

    public AbstractDeferredValueRecordCursorFactory(
            @NotNull RecordMetadata metadata,
            @NotNull DataFrameCursorFactory dataFrameCursorFactory,
            int columnIndex,
            Function symbolFunc,
            @Nullable Function filter,
            IntList columnIndexes
    ) {
        super(metadata, dataFrameCursorFactory);
        this.columnIndex = columnIndex;
        this.symbolFunc = symbolFunc;
        this.filter = filter;
        this.columnIndexes = columnIndexes;
    }

    @Override
    protected void _close() {
        super._close();
        if (filter != null) {
            filter.close();
        }
    }

    protected abstract AbstractDataFrameRecordCursor createDataFrameCursorFor(int symbolKey);

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (cursor == null && lookupDeferredSymbol(dataFrameCursor)) {
            if (recordCursorSupportsRandomAccess()) {
                return EmptyTableRandomRecordCursor.INSTANCE;
            }
            return EmptyTableRecordCursor.INSTANCE;
        }
        cursor.of(dataFrameCursor, executionContext);
        return cursor;
    }

    private boolean lookupDeferredSymbol(DataFrameCursor dataFrameCursor) {
        final CharSequence symbol = symbolFunc.getStr(null);
        int symbolKey = dataFrameCursor.getSymbolTable(columnIndexes.get(columnIndex)).keyOf(symbol);
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            dataFrameCursor.close();
            return true;
        }

        this.cursor = createDataFrameCursorFor(symbolKey);
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.optAttr("filter", filter);
        sink.attr("symbolFilter").putColumnName(columnIndex).put('=').put(symbolFunc);
        sink.child(dataFrameCursorFactory);
    }
}
