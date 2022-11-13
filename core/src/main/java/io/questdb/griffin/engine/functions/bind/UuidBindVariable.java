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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.Mutable;

public class UuidBindVariable extends UuidFunction implements ScalarFunction, Mutable {
    long leastSigBits = UuidConstant.NULL_MSB_AND_LSB;
    long mostSigBits = UuidConstant.NULL_MSB_AND_LSB;

    @Override
    public void clear() {

    }

    @Override
    public long getUuidLeastSig(Record rec) {
        return leastSigBits;
    }

    @Override
    public long getUuidMostSig(Record rec) {
        return mostSigBits;
    }

    void set(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }
}
