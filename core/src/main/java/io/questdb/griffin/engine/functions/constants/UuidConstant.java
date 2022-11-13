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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.std.MutableUuid;
import io.questdb.std.Numbers;

public class UuidConstant extends UuidFunction implements ConstantFunction {
    public static final long NULL_MSB_AND_LSB = Numbers.LONG_NaN;
    public final static UuidConstant NULL = new UuidConstant(NULL_MSB_AND_LSB, NULL_MSB_AND_LSB);
    private final long leastSigBits;
    private final long mostSigBits;

    public UuidConstant(MutableUuid that) {
        this(that.getMostSigBits(), that.getLeastSigBits());
    }

    public UuidConstant(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

    @Override
    public long getUuidLeastSig(Record rec) {
        return leastSigBits;
    }

    @Override
    public long getUuidMostSig(Record rec) {
        return mostSigBits;
    }
}
