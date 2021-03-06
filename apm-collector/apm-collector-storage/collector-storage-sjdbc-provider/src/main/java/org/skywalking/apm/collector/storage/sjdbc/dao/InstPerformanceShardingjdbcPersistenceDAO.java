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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.IInstPerformancePersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.skywalking.apm.collector.storage.table.instance.InstPerformance;
import org.skywalking.apm.collector.storage.table.instance.InstPerformanceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class InstPerformanceShardingjdbcPersistenceDAO extends ShardingjdbcDAO implements IInstPerformancePersistenceDAO<ShardingjdbcSqlEntity, ShardingjdbcSqlEntity, InstPerformance> {

    private final Logger logger = LoggerFactory.getLogger(InstPerformanceShardingjdbcPersistenceDAO.class);
    private static final String GET_SQL = "select * from {0} where {1} = ?";

    public InstPerformanceShardingjdbcPersistenceDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public InstPerformance get(String id) {
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_SQL, InstPerformanceTable.TABLE, InstPerformanceTable.COLUMN_ID);
        Object[] params = new Object[] {id};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                InstPerformance instPerformance = new InstPerformance(id);
                instPerformance.setApplicationId(rs.getInt(InstPerformanceTable.COLUMN_APPLICATION_ID));
                instPerformance.setInstanceId(rs.getInt(InstPerformanceTable.COLUMN_INSTANCE_ID));
                instPerformance.setCalls(rs.getInt(InstPerformanceTable.COLUMN_CALLS));
                instPerformance.setCostTotal(rs.getLong(InstPerformanceTable.COLUMN_COST_TOTAL));
                instPerformance.setTimeBucket(rs.getLong(InstPerformanceTable.COLUMN_TIME_BUCKET));
                return instPerformance;
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchInsert(InstPerformance data) {
        Map<String, Object> source = new HashMap<>();
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        source.put(InstPerformanceTable.COLUMN_ID, data.getId());
        source.put(InstPerformanceTable.COLUMN_APPLICATION_ID, data.getApplicationId());
        source.put(InstPerformanceTable.COLUMN_INSTANCE_ID, data.getInstanceId());
        source.put(InstPerformanceTable.COLUMN_CALLS, data.getCalls());
        source.put(InstPerformanceTable.COLUMN_COST_TOTAL, data.getCostTotal());
        source.put(InstPerformanceTable.COLUMN_TIME_BUCKET, data.getTimeBucket());
        String sql = SqlBuilder.buildBatchInsertSql(InstPerformanceTable.TABLE, source.keySet());
        entity.setSql(sql);
        entity.setParams(source.values().toArray(new Object[0]));
        return entity;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchUpdate(InstPerformance data) {
        Map<String, Object> source = new HashMap<>();
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        source.put(InstPerformanceTable.COLUMN_APPLICATION_ID, data.getApplicationId());
        source.put(InstPerformanceTable.COLUMN_INSTANCE_ID, data.getInstanceId());
        source.put(InstPerformanceTable.COLUMN_CALLS, data.getCalls());
        source.put(InstPerformanceTable.COLUMN_COST_TOTAL, data.getCostTotal());
        source.put(InstPerformanceTable.COLUMN_TIME_BUCKET, data.getTimeBucket());
        String sql = SqlBuilder.buildBatchUpdateSql(InstPerformanceTable.TABLE, source.keySet(), InstPerformanceTable.COLUMN_ID);
        entity.setSql(sql);
        List<Object> values = new ArrayList<>(source.values());
        values.add(data.getId());
        entity.setParams(values.toArray(new Object[0]));
        return entity;
    }
}
