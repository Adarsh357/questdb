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

import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.str.Path;
import org.junit.Test;

public class WalWriterTest extends AbstractGriffinTest {

    @Test
    public void bootstrapWal() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE).col("a", ColumnType.INT)
        ) {
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                for (int i = 0; i < 100; i++) {
                    WalWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
            }
            System.out.println("done");
        }
    }
}