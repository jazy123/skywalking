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
import org.skywalking.apm.collector.storage.dao.IServiceEntryPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.skywalking.apm.collector.storage.table.service.ServiceEntry;
import org.skywalking.apm.collector.storage.table.service.ServiceEntryTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class ServiceEntryShardingjdbcPersistenceDAO extends ShardingjdbcDAO implements IServiceEntryPersistenceDAO<ShardingjdbcSqlEntity, ShardingjdbcSqlEntity, ServiceEntry> {

    private final Logger logger = LoggerFactory.getLogger(ServiceEntryShardingjdbcPersistenceDAO.class);
    private static final String GET_SERVICE_ENTRY_SQL = "select * from {0} where {1} = ?";

    public ServiceEntryShardingjdbcPersistenceDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public ServiceEntry get(String id) {
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_SERVICE_ENTRY_SQL, ServiceEntryTable.TABLE, ServiceEntryTable.COLUMN_ID);
        Object[] params = new Object[] {id};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                ServiceEntry serviceEntry = new ServiceEntry(id);
                serviceEntry.setApplicationId(rs.getInt(ServiceEntryTable.COLUMN_APPLICATION_ID));
                serviceEntry.setEntryServiceId(rs.getInt(ServiceEntryTable.COLUMN_ENTRY_SERVICE_ID));
                serviceEntry.setEntryServiceName(rs.getString(ServiceEntryTable.COLUMN_ENTRY_SERVICE_NAME));
                serviceEntry.setRegisterTime(rs.getLong(ServiceEntryTable.COLUMN_REGISTER_TIME));
                serviceEntry.setNewestTime(rs.getLong(ServiceEntryTable.COLUMN_NEWEST_TIME));
                return serviceEntry;
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchInsert(ServiceEntry data) {
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        Map<String, Object> source = new HashMap<>();
        source.put(ServiceEntryTable.COLUMN_ID, data.getId());
        source.put(ServiceEntryTable.COLUMN_APPLICATION_ID, data.getApplicationId());
        source.put(ServiceEntryTable.COLUMN_ENTRY_SERVICE_ID, data.getEntryServiceId());
        source.put(ServiceEntryTable.COLUMN_ENTRY_SERVICE_NAME, data.getEntryServiceName());
        source.put(ServiceEntryTable.COLUMN_REGISTER_TIME, data.getRegisterTime());
        source.put(ServiceEntryTable.COLUMN_NEWEST_TIME, data.getNewestTime());
        String sql = SqlBuilder.buildBatchInsertSql(ServiceEntryTable.TABLE, source.keySet());
        entity.setSql(sql);
        entity.setParams(source.values().toArray(new Object[0]));
        return entity;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchUpdate(ServiceEntry data) {
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        Map<String, Object> source = new HashMap<>();
        source.put(ServiceEntryTable.COLUMN_APPLICATION_ID, data.getApplicationId());
        source.put(ServiceEntryTable.COLUMN_ENTRY_SERVICE_ID, data.getEntryServiceId());
        source.put(ServiceEntryTable.COLUMN_ENTRY_SERVICE_NAME, data.getEntryServiceName());
        source.put(ServiceEntryTable.COLUMN_REGISTER_TIME, data.getRegisterTime());
        source.put(ServiceEntryTable.COLUMN_NEWEST_TIME, data.getNewestTime());
        String sql = SqlBuilder.buildBatchUpdateSql(ServiceEntryTable.TABLE, source.keySet(), ServiceEntryTable.COLUMN_ID);
        entity.setSql(sql);
        List<Object> values = new ArrayList<>(source.values());
        values.add(data.getId());
        entity.setParams(values.toArray(new Object[0]));
        return entity;
    }
}
