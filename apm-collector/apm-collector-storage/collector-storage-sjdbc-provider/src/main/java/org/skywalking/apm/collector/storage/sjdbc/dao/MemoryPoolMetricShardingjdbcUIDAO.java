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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.core.util.Const;
import org.skywalking.apm.collector.core.util.TimeBucketUtils;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.IMemoryPoolMetricUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.jvm.MemoryPoolMetricTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * @author linjiaqi
 */
public class MemoryPoolMetricShardingjdbcUIDAO extends ShardingjdbcDAO implements IMemoryPoolMetricUIDAO {

    private final Logger logger = LoggerFactory.getLogger(MemoryPoolMetricShardingjdbcUIDAO.class);
    private static final String GET_MEMORY_POOL_METRIC_SQL = "select * from {0} where {1} = ?";

    public MemoryPoolMetricShardingjdbcUIDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public JsonObject getMetric(int instanceId, long timeBucket, int poolType) {
        ShardingjdbcClient client = getClient();
        String id = timeBucket + Const.ID_SPLIT + instanceId + Const.ID_SPLIT + poolType;
        String sql = SqlBuilder.buildSql(GET_MEMORY_POOL_METRIC_SQL, MemoryPoolMetricTable.TABLE, MemoryPoolMetricTable.COLUMN_ID);
        Object[] params = new Object[] {id};
        JsonObject metric = new JsonObject();
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                metric.addProperty("max", rs.getInt(MemoryPoolMetricTable.COLUMN_MAX));
                metric.addProperty("init", rs.getInt(MemoryPoolMetricTable.COLUMN_INIT));
                metric.addProperty("used", rs.getInt(MemoryPoolMetricTable.COLUMN_USED));
            } else {
                metric.addProperty("max", 0);
                metric.addProperty("init", 0);
                metric.addProperty("used", 0);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return metric;
    }

    @Override public JsonObject getMetric(int instanceId, long startTimeBucket, long endTimeBucket, int poolType) {
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_MEMORY_POOL_METRIC_SQL, MemoryPoolMetricTable.TABLE, MemoryPoolMetricTable.COLUMN_ID);
        List<String> idList = new ArrayList<>();
        long timeBucket = startTimeBucket;
        do {
            timeBucket = TimeBucketUtils.INSTANCE.addSecondForSecondTimeBucket(TimeBucketUtils.TimeBucketType.SECOND.name(), timeBucket, 1);
            String id = timeBucket + Const.ID_SPLIT + instanceId + Const.ID_SPLIT + poolType;
            idList.add(id);
        }
        while (timeBucket <= endTimeBucket);

        JsonObject metric = new JsonObject();
        JsonArray usedMetric = new JsonArray();

        idList.forEach(id -> {
            try (
                    ResultSet rs = client.executeQuery(sql, new String[] {id});
                    Statement st = rs.getStatement();
                    Connection conn = st.getConnection();
                ) {
                if (rs.next()) {
                    metric.addProperty("max", rs.getLong(MemoryPoolMetricTable.COLUMN_MAX));
                    metric.addProperty("init", rs.getLong(MemoryPoolMetricTable.COLUMN_INIT));
                    usedMetric.add(rs.getLong(MemoryPoolMetricTable.COLUMN_USED));
                } else {
                    metric.addProperty("max", 0);
                    metric.addProperty("init", 0);
                    usedMetric.add(0);
                }
            } catch (SQLException | ShardingjdbcClientException e) {
                logger.error(e.getMessage(), e);
            }
        });

        metric.add("used", usedMetric);
        return metric;
    }
}
