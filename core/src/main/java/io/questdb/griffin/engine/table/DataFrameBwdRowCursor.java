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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.RowCursor;

/**
 * Row cursor that goes through data frame backwards / from end to start / hi to lo .
 */
public class DataFrameBwdRowCursor implements RowCursor {
    private long lo;
    private long current;

    @Override
    public boolean hasNext() {
        return current >= lo;
    }

    @Override
    public long next() {
        return current--;
    }

    void of(DataFrame frame) {
        this.current = frame.getRowHi() - 1;
        this.lo = frame.getRowLo();
    }

    @Override
    public void jumpTo(long position) {
        this.current = position;
    }
}
