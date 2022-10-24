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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReducer;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.*;

public class AsyncJitFilteredRecordCursorFactory extends AbstractRecordCursorFactory {

    private static final PageFrameReducer REDUCER = AsyncJitFilteredRecordCursorFactory::filter;

    private final RecordCursorFactory base;
    private final AsyncFilteredRecordCursor cursor;
    private final AsyncFilteredNegativeLimitRecordCursor negativeLimitCursor;
    private final AsyncJitFilterAtom filterAtom;
    private final PageFrameSequence<AsyncJitFilterAtom> frameSequence;
    private final SCSequence collectSubSeq = new SCSequence();
    private final Function limitLoFunction;
    private final int limitLoPos;
    private final int maxNegativeLimit;
    private DirectLongList negativeLimitRows;
    private final boolean preTouchColumns;
    private final int workerCount;

    public AsyncJitFilteredRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull ObjList<Function> bindVarFunctions,
            @NotNull Function filter,
            @Nullable ObjList<Function> perWorkerFilters,
            @NotNull CompiledFilter compiledFilter,
            @NotNull @Transient WeakClosableObjectPool<PageFrameReduceTask> localTaskPool,
            @Nullable Function limitLoFunction,
            int limitLoPos,
            boolean preTouchColumns,
            int workerCount
    ) {
        super(base.getMetadata());
        assert !(base instanceof FilteredRecordCursorFactory);
        assert !(base instanceof AsyncJitFilteredRecordCursorFactory);
        this.base = base;
        this.cursor = new AsyncFilteredRecordCursor(filter, base.hasDescendingOrder());
        this.negativeLimitCursor = new AsyncFilteredNegativeLimitRecordCursor();
        MemoryCARW bindVarMemory = Vm.getCARWInstance(configuration.getSqlJitBindVarsMemoryPageSize(),
                configuration.getSqlJitBindVarsMemoryMaxPages(), MemoryTag.NATIVE_JIT);
        IntList preTouchColumnTypes = null;
        if (preTouchColumns) {
            preTouchColumnTypes = new IntList();
            for (int i = 0, n = base.getMetadata().getColumnCount(); i < n; i++) {
                int columnType = base.getMetadata().getColumnType(i);
                preTouchColumnTypes.add(columnType);
            }
        }
        this.filterAtom = new AsyncJitFilterAtom(filter, perWorkerFilters, compiledFilter, bindVarMemory, bindVarFunctions, preTouchColumnTypes);
        this.frameSequence = new PageFrameSequence<>(configuration, messageBus, REDUCER, localTaskPool);
        this.limitLoFunction = limitLoFunction;
        this.limitLoPos = limitLoPos;
        this.maxNegativeLimit = configuration.getSqlMaxNegativeLimit();
        this.preTouchColumns = preTouchColumns;
        this.workerCount = workerCount;
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(filterAtom);
        Misc.free(frameSequence);
        Misc.free(negativeLimitRows);
        cursor.freeRecords();
        negativeLimitCursor.freeRecords();
    }

    @Override
    public boolean followedLimitAdvice() {
        return limitLoFunction != null;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        long rowsRemaining;
        int baseOrder = base.hasDescendingOrder() ? ORDER_DESC : ORDER_ASC;
        final int order;
        if (limitLoFunction != null) {
            limitLoFunction.init(frameSequence.getSymbolTableSource(), executionContext);
            rowsRemaining = limitLoFunction.getLong(null);
            // on negative limit we will be looking for positive number of rows
            // while scanning table from the highest timestamp to the lowest
            if (rowsRemaining > -1) {
                order = baseOrder;
            } else {
                order = reverse(baseOrder);
                rowsRemaining = -rowsRemaining;
            }
        } else {
            rowsRemaining = Long.MAX_VALUE;
            order = baseOrder;
        }

        if (order == ORDER_DESC && rowsRemaining != Long.MAX_VALUE) {
            if (rowsRemaining > maxNegativeLimit) {
                throw SqlException.position(limitLoPos).put("absolute LIMIT value is too large, maximum allowed value: ").put(maxNegativeLimit);
            }
            if (negativeLimitRows == null) {
                negativeLimitRows = new DirectLongList(maxNegativeLimit, MemoryTag.NATIVE_OFFLOAD);
            }
            negativeLimitCursor.of(execute(executionContext, collectSubSeq, order), rowsRemaining, negativeLimitRows);
            return negativeLimitCursor;
        }

        cursor.of(execute(executionContext, collectSubSeq, order), rowsRemaining);
        return cursor;
    }

    @Override
    public PageFrameSequence<AsyncJitFilterAtom> execute(SqlExecutionContext executionContext, Sequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, filterAtom, order);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsUpdateRowId(CharSequence tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public boolean usesCompiledFilter() {
        return true;
    }

    public boolean hasDescendingOrder() {
        return base.hasDescendingOrder();
    }

    private static void filter(
            int workerId,
            @NotNull PageAddressCacheRecord record,
            @NotNull PageFrameReduceTask task,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker,
            @Nullable PageFrameSequence<?> stealingFrameSequence
    ) {
        final DirectLongList rows = task.getRows();
        final DirectLongList columns = task.getColumns();
        final long frameRowCount = task.getFrameRowCount();
        final AsyncJitFilterAtom atom = task.getFrameSequence(AsyncJitFilterAtom.class).getAtom();
        final PageAddressCache pageAddressCache = task.getPageAddressCache();

        rows.clear();

        if (pageAddressCache.hasColumnTops(task.getFrameIndex())) {
            // Use Java-based filter in case of a page frame with column tops.
            final boolean owner = stealingFrameSequence != null && stealingFrameSequence == task.getFrameSequence();
            final int filterId = atom.acquireFilter(workerId, owner, circuitBreaker);
            final Function filter = atom.getFilter(filterId);
            try {
                for (long r = 0; r < frameRowCount; r++) {
                    record.setRowIndex(r);
                    if (filter.getBool(record)) {
                        rows.add(r);
                    }
                }
                return;
            } finally {
                atom.releaseFilter(filterId);
            }
        }

        // Use JIT-compiled filter.

        final long columnCount = pageAddressCache.getColumnCount();
        if (columns.getCapacity() < columnCount) {
            columns.setCapacity(columnCount);
        }
        columns.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columns.add(pageAddressCache.getPageAddress(task.getFrameIndex(), columnIndex));
        }

        final long rowCount = task.getFrameRowCount();
        if (rows.getCapacity() < rowCount) {
            rows.setCapacity(rowCount);
        }

        long hi = atom.compiledFilter.call(
                columns.getAddress(),
                columns.size(),
                atom.bindVarMemory.getAddress(),
                atom.bindVarFunctions.size(),
                rows.getAddress(),
                rowCount,
                0
        );
        rows.setPos(hi);

        // Pre-touch fixed-size columns, if asked.
        atom.preTouchColumns(record, rows);
    }

    private static class AsyncJitFilterAtom extends AsyncFilterAtom implements Sinkable {

        final CompiledFilter compiledFilter;
        final MemoryCARW bindVarMemory;
        final ObjList<Function> bindVarFunctions;

        public AsyncJitFilterAtom(
                Function filter,
                ObjList<Function> perWorkerFilters,
                CompiledFilter compiledFilter,
                MemoryCARW bindVarMemory,
                ObjList<Function> bindVarFunctions,
                @Nullable IntList preTouchColumnTypes
        ) {
            super(filter, perWorkerFilters, preTouchColumnTypes);
            this.compiledFilter = compiledFilter;
            this.bindVarMemory = bindVarMemory;
            this.bindVarFunctions = bindVarFunctions;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(bindVarFunctions, symbolTableSource, executionContext);
            prepareBindVarMemory(symbolTableSource, executionContext);
        }

        @Override
        public void close() {
            super.close();
            Misc.free(compiledFilter);
            Misc.free(bindVarMemory);
            Misc.freeObjList(bindVarFunctions);
        }

        @Override
        public void toSink(CharSink sink) {
            super.toSink(sink);
        }

        private void prepareBindVarMemory(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            //don't trigger memory allocation if there are no variables 
            if (bindVarFunctions.size() > 0) {
                bindVarMemory.truncate();
                for (int i = 0, n = bindVarFunctions.size(); i < n; i++) {
                    Function function = bindVarFunctions.getQuick(i);
                    writeBindVarFunction(function, symbolTableSource, executionContext);
                }
            }
        }

        private void writeBindVarFunction(
                Function function,
                SymbolTableSource symbolTableSource,
                SqlExecutionContext executionContext
        ) throws SqlException {
            final int columnType = function.getType();
            final int columnTypeTag = ColumnType.tagOf(columnType);
            switch (columnTypeTag) {
                case ColumnType.BOOLEAN:
                    bindVarMemory.putLong(function.getBool(null) ? 1 : 0);
                    return;
                case ColumnType.BYTE:
                    bindVarMemory.putLong(function.getByte(null));
                    return;
                case ColumnType.GEOBYTE:
                    bindVarMemory.putLong(function.getGeoByte(null));
                    return;
                case ColumnType.SHORT:
                    bindVarMemory.putLong(function.getShort(null));
                    return;
                case ColumnType.GEOSHORT:
                    bindVarMemory.putLong(function.getGeoShort(null));
                    return;
                case ColumnType.CHAR:
                    bindVarMemory.putLong(function.getChar(null));
                    return;
                case ColumnType.INT:
                    bindVarMemory.putLong(function.getInt(null));
                    return;
                case ColumnType.GEOINT:
                    bindVarMemory.putLong(function.getGeoInt(null));
                    return;
                case ColumnType.SYMBOL:
                    assert function instanceof CompiledFilterSymbolBindVariable;
                    function.init(symbolTableSource, executionContext);
                    bindVarMemory.putLong(function.getInt(null));
                    return;
                case ColumnType.FLOAT:
                    // compiled filter function will read only the first word
                    bindVarMemory.putFloat(function.getFloat(null));
                    bindVarMemory.putFloat(Float.NaN);
                    return;
                case ColumnType.LONG:
                    bindVarMemory.putLong(function.getLong(null));
                    return;
                case ColumnType.GEOLONG:
                    bindVarMemory.putLong(function.getGeoLong(null));
                    return;
                case ColumnType.DATE:
                    bindVarMemory.putLong(function.getDate(null));
                    return;
                case ColumnType.TIMESTAMP:
                    bindVarMemory.putLong(function.getTimestamp(null));
                    return;
                case ColumnType.DOUBLE:
                    bindVarMemory.putDouble(function.getDouble(null));
                    return;
                default:
                    throw SqlException.position(0).put("unsupported bind variable type: ").put(ColumnType.nameOf(columnTypeTag));
            }
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("async jit filter");
        if (limitLoFunction != null) {
            sink.attr("limit").val(limitLoFunction);
        }
        sink.attr("filter").val(filterAtom);
        sink.attr("preTouch").val(preTouchColumns);
        sink.attr("workers").val(workerCount);
        sink.child(base);
    }
}
