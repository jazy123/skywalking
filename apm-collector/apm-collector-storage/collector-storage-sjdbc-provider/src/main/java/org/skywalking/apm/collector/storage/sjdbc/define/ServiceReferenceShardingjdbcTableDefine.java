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
import org.skywalking.apm.collector.storage.table.serviceref.ServiceReferenceTable;

/**
 * @author linjiaqi
 */
public class ServiceReferenceShardingjdbcTableDefine extends ShardingjdbcTableDefine {

    public ServiceReferenceShardingjdbcTableDefine() {
        super(ServiceReferenceTable.TABLE);
    }

    @Override public void initialize() {
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_ID, ShardingjdbcColumnDefine.Type.Varchar.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_ENTRY_SERVICE_ID, ShardingjdbcColumnDefine.Type.Int.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_FRONT_SERVICE_ID, ShardingjdbcColumnDefine.Type.Int.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_BEHIND_SERVICE_ID, ShardingjdbcColumnDefine.Type.Int.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_S1_LTE, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_S3_LTE, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_S5_LTE, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_S5_GT, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_SUMMARY, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_ERROR, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_COST_SUMMARY, ShardingjdbcColumnDefine.Type.Bigint.name()));
        addColumn(new ShardingjdbcColumnDefine(ServiceReferenceTable.COLUMN_TIME_BUCKET, ShardingjdbcColumnDefine.Type.Bigint.name()));
    }
}
