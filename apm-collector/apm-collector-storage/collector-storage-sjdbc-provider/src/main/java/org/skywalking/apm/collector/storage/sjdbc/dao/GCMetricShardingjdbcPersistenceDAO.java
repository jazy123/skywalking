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
import org.skywalking.apm.collector.storage.dao.IGCMetricPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.skywalking.apm.collector.storage.table.jvm.GCMetric;
import org.skywalking.apm.collector.storage.table.jvm.GCMetricTable;

/**
 * @author linjiaqi
 */
public class GCMetricShardingjdbcPersistenceDAO extends ShardingjdbcDAO implements IGCMetricPersistenceDAO<ShardingjdbcSqlEntity, ShardingjdbcSqlEntity, GCMetric> {

    public GCMetricShardingjdbcPersistenceDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public GCMetric get(String id) {
        return null;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchInsert(GCMetric data) {
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        Map<String, Object> source = new HashMap<>();
        source.put(GCMetricTable.COLUMN_ID, data.getId());
        source.put(GCMetricTable.COLUMN_INSTANCE_ID, data.getInstanceId());
        source.put(GCMetricTable.COLUMN_PHRASE, data.getPhrase());
        source.put(GCMetricTable.COLUMN_COUNT, data.getCount());
        source.put(GCMetricTable.COLUMN_TIME, data.getTime());
        source.put(GCMetricTable.COLUMN_TIME_BUCKET, data.getTimeBucket());

        String sql = SqlBuilder.buildBatchInsertSql(GCMetricTable.TABLE, source.keySet());
        entity.setSql(sql);
        entity.setParams(source.values().toArray(new Object[0]));
        return entity;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchUpdate(GCMetric data) {
        return null;
    }
}
