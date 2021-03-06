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
import org.skywalking.apm.collector.storage.dao.IInstPerformanceUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.instance.InstPerformanceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;

/**
 * @author linjiaqi
 */
public class InstPerformanceShardingjdbcUIDAO extends ShardingjdbcDAO implements IInstPerformanceUIDAO {

    private final Logger logger = LoggerFactory.getLogger(InstPerformanceShardingjdbcUIDAO.class);
    private static final String GET_INST_PERF_SQL = "select * from {0} where {1} = ? and {2} in (";
    private static final String GET_TPS_METRIC_SQL = "select * from {0} where {1} = ?";

    public InstPerformanceShardingjdbcUIDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public InstPerformance get(long[] timeBuckets, int instanceId) {
        ShardingjdbcClient client = getClient();
        logger.info("the inst performance inst id = {}", instanceId);
        String sql = SqlBuilder.buildSql(GET_INST_PERF_SQL, InstPerformanceTable.TABLE, InstPerformanceTable.COLUMN_INSTANCE_ID, InstPerformanceTable.COLUMN_TIME_BUCKET);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < timeBuckets.length; i++) {
            builder.append("?,");
        }
        builder.delete(builder.length() - 1, builder.length());
        builder.append(")");
        sql = sql + builder;
        Object[] params = new Object[timeBuckets.length + 1];
        for (int i = 0; i < timeBuckets.length; i++) {
            params[i + 1] = timeBuckets[i];
        }
        params[0] = instanceId;
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                int callTimes = rs.getInt(InstPerformanceTable.COLUMN_CALLS);
                int costTotal = rs.getInt(InstPerformanceTable.COLUMN_COST_TOTAL);
                return new InstPerformance(instanceId, callTimes, costTotal);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override public int getTpsMetric(int instanceId, long timeBucket) {
        logger.info("getTpMetric instanceId = {}, startTimeBucket = {}", instanceId, timeBucket);
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_TPS_METRIC_SQL, InstPerformanceTable.TABLE, InstPerformanceTable.COLUMN_ID);
        Object[] params = new Object[] {instanceId};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                return rs.getInt(InstPerformanceTable.COLUMN_CALLS);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }

    @Override public JsonArray getTpsMetric(int instanceId, long startTimeBucket, long endTimeBucket) {
        logger.info("getTpsMetric instanceId = {}, startTimeBucket = {}, endTimeBucket = {}", instanceId, startTimeBucket, endTimeBucket);
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_TPS_METRIC_SQL, InstPerformanceTable.TABLE, InstPerformanceTable.COLUMN_ID);

        long timeBucket = startTimeBucket;
        List<String> idList = new ArrayList<>();
        do {
            String id = timeBucket + Const.ID_SPLIT + instanceId;
            timeBucket = TimeBucketUtils.INSTANCE.addSecondForSecondTimeBucket(TimeBucketUtils.TimeBucketType.SECOND.name(), timeBucket, 1);
            idList.add(id);
        }
        while (timeBucket <= endTimeBucket);

        JsonArray metrics = new JsonArray();
        idList.forEach(id -> {
            try (
                    ResultSet rs = client.executeQuery(sql, new Object[] {id});
                    Statement st = rs.getStatement();
                    Connection conn = st.getConnection();
                ) {
                if (rs.next()) {
                    int calls = rs.getInt(InstPerformanceTable.COLUMN_CALLS);
                    metrics.add(calls);
                } else {
                    metrics.add(0);
                }
            } catch (SQLException | ShardingjdbcClientException e) {
                logger.error(e.getMessage(), e);
            }
        });
        return metrics;
    }

    @Override public int getRespTimeMetric(int instanceId, long timeBucket) {
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_TPS_METRIC_SQL, InstPerformanceTable.TABLE, InstPerformanceTable.COLUMN_ID);
        Object[] params = new Object[] {instanceId};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                int callTimes = rs.getInt(InstPerformanceTable.COLUMN_CALLS);
                int costTotal = rs.getInt(InstPerformanceTable.COLUMN_COST_TOTAL);
                return costTotal / callTimes;
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }

    @Override public JsonArray getRespTimeMetric(int instanceId, long startTimeBucket, long endTimeBucket) {
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_TPS_METRIC_SQL, InstPerformanceTable.TABLE, InstPerformanceTable.COLUMN_ID);

        long timeBucket = startTimeBucket;
        List<String> idList = new ArrayList<>();
        do {
            String id = timeBucket + Const.ID_SPLIT + instanceId;
            timeBucket = TimeBucketUtils.INSTANCE.addSecondForSecondTimeBucket(TimeBucketUtils.TimeBucketType.SECOND.name(), timeBucket, 1);
            idList.add(id);
        }
        while (timeBucket <= endTimeBucket);

        JsonArray metrics = new JsonArray();
        idList.forEach(id -> {
            try (
                    ResultSet rs = client.executeQuery(sql, new Object[] {id});
                    Statement st = rs.getStatement();
                    Connection conn = st.getConnection();
                ) {
                if (rs.next()) {
                    int callTimes = rs.getInt(InstPerformanceTable.COLUMN_CALLS);
                    int costTotal = rs.getInt(InstPerformanceTable.COLUMN_COST_TOTAL);
                    metrics.add(costTotal / callTimes);
                } else {
                    metrics.add(0);
                }
            } catch (SQLException | ShardingjdbcClientException e) {
                logger.error(e.getMessage(), e);
            }
        });
        return metrics;
    }
}
