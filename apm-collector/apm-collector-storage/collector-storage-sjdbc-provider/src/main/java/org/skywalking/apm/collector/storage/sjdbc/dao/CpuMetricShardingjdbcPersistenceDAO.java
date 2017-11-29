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

package org.skywalking.apm.collector.storage.sjdbc.dao;

import java.util.HashMap;
import java.util.Map;

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.ICpuMetricPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.skywalking.apm.collector.storage.table.jvm.CpuMetric;
import org.skywalking.apm.collector.storage.table.jvm.CpuMetricTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class CpuMetricShardingjdbcPersistenceDAO extends ShardingjdbcDAO implements ICpuMetricPersistenceDAO<ShardingjdbcSqlEntity, ShardingjdbcSqlEntity, CpuMetric> {

    private final Logger logger = LoggerFactory.getLogger(CpuMetricShardingjdbcPersistenceDAO.class);

    public CpuMetricShardingjdbcPersistenceDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public CpuMetric get(String id) {
        return null;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchInsert(CpuMetric data) {
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        Map<String, Object> source = new HashMap<>();
        source.put(CpuMetricTable.COLUMN_ID, data.getId());
        source.put(CpuMetricTable.COLUMN_INSTANCE_ID, data.getInstanceId());
        source.put(CpuMetricTable.COLUMN_USAGE_PERCENT, data.getUsagePercent());
        source.put(CpuMetricTable.COLUMN_TIME_BUCKET, data.getTimeBucket());

        logger.debug("prepare cpu metric batch insert, getId: {}", data.getId());
        String sql = SqlBuilder.buildBatchInsertSql(CpuMetricTable.TABLE, source.keySet());
        entity.setSql(sql);
        entity.setParams(source.values().toArray(new Object[0]));
        return entity;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchUpdate(CpuMetric data) {
        return null;
    }
}
