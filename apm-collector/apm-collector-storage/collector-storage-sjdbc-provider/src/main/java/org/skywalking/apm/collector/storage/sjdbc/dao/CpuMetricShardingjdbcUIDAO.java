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
import org.skywalking.apm.collector.storage.dao.ICpuMetricUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.jvm.CpuMetricTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;

/**
 * @author linjiaqi
 */
public class CpuMetricShardingjdbcUIDAO extends ShardingjdbcDAO implements ICpuMetricUIDAO {
    private final Logger logger = LoggerFactory.getLogger(CpuMetricShardingjdbcUIDAO.class);
    private static final String GET_CPU_METRIC_SQL = "select * from {0} where {1} = ?";

    public CpuMetricShardingjdbcUIDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public int getMetric(int instanceId, long timeBucket) {
        String id = timeBucket + Const.ID_SPLIT + instanceId;
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_CPU_METRIC_SQL, CpuMetricTable.TABLE, CpuMetricTable.COLUMN_ID);
        Object[] params = new Object[] {id};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                return rs.getInt(CpuMetricTable.COLUMN_USAGE_PERCENT);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }

    @Override public JsonArray getMetric(int instanceId, long startTimeBucket, long endTimeBucket) {
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_CPU_METRIC_SQL, CpuMetricTable.TABLE, CpuMetricTable.COLUMN_ID);

        long timeBucket = startTimeBucket;
        List<String> idList = new ArrayList<>();
        do {
            timeBucket = TimeBucketUtils.INSTANCE.addSecondForSecondTimeBucket(TimeBucketUtils.TimeBucketType.SECOND.name(), timeBucket, 1);
            String id = timeBucket + Const.ID_SPLIT + instanceId;
            idList.add(id);
        }
        while (timeBucket <= endTimeBucket);

        JsonArray metrics = new JsonArray();
        idList.forEach(id -> {
            try (
                    ResultSet rs = client.executeQuery(sql, new String[] {id});
                    Statement st = rs.getStatement();
                    Connection conn = st.getConnection();
                ) {
                if (rs.next()) {
                    double cpuUsed = rs.getDouble(CpuMetricTable.COLUMN_USAGE_PERCENT);
                    metrics.add((int)(cpuUsed * 100));
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
