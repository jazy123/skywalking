/*
 * Copyright 2017, OpenSkywalking Organization All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Project repository: https://github.com/OpenSkywalking/skywalking
 */

package org.skywalking.apm.collector.storage.sjdbc.define;

import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcColumnDefine;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcTableDefine;
import org.skywalking.apm.collector.storage.table.register.InstanceTable;

/**
 * @author linjiaqi
 */
public class InstanceShardingjdbcTableDefine extends ShardingjdbcTableDefine {

    public InstanceShardingjdbcTableDefine() {
        super(InstanceTable.TABLE);
    }

    @Override public void initialize() {
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_ID, ShardingjdbcColumnDefine.Type.Varchar.name()));
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_APPLICATION_ID, ShardingjdbcColumnDefine.Type.Int.name()));
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_AGENT_UUID, ShardingjdbcColumnDefine.Type.Varchar.name()));
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_REGISTER_TIME, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_INSTANCE_ID, ShardingjdbcColumnDefine.Type.Int.name()));
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_HEARTBEAT_TIME, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(InstanceTable.COLUMN_OS_INFO, ShardingjdbcColumnDefine.Type.Varchar.name()));
    }
}
