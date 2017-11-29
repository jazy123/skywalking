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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.core.UnexpectedException;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.IInstanceHeartBeatPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.skywalking.apm.collector.storage.table.register.Instance;
import org.skywalking.apm.collector.storage.table.register.InstanceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class InstanceHeartBeatShardingjdbcPersistenceDAO extends ShardingjdbcDAO implements IInstanceHeartBeatPersistenceDAO<ShardingjdbcSqlEntity, ShardingjdbcSqlEntity, Instance> {

    private final Logger logger = LoggerFactory.getLogger(InstanceHeartBeatShardingjdbcPersistenceDAO.class);

    public InstanceHeartBeatShardingjdbcPersistenceDAO(ShardingjdbcClient client) {
        super(client);
    }

    private static final String GET_INSTANCE_HEARTBEAT_SQL = "select * from {0} where {1} = ?";

    @Override public Instance get(String id) {
    	ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_INSTANCE_HEARTBEAT_SQL, InstanceTable.TABLE, InstanceTable.COLUMN_INSTANCE_ID);
        Object[] params = new Object[] {id};
        try (ResultSet rs = client.executeQuery(sql, params)) {
            if (rs.next()) {
                Instance instance = new Instance(id);
                instance.setInstanceId(rs.getInt(InstanceTable.COLUMN_INSTANCE_ID));
                instance.setHeartBeatTime(rs.getLong(InstanceTable.COLUMN_HEARTBEAT_TIME));
                return instance;
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchInsert(Instance data) {
        throw new UnexpectedException("There is no need to merge stream data with database data.");
    }

    @Override public ShardingjdbcSqlEntity prepareBatchUpdate(Instance data) {
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        Map<String, Object> source = new HashMap<>();
        source.put(InstanceTable.COLUMN_HEARTBEAT_TIME, data.getHeartBeatTime());
        String sql = SqlBuilder.buildBatchUpdateSql(InstanceTable.TABLE, source.keySet(), InstanceTable.COLUMN_INSTANCE_ID);
        entity.setSql(sql);
        List<Object> params = new ArrayList<>(source.values());
        params.add(data.getId());
        entity.setParams(params.toArray(new Object[0]));
        return entity;
    }
}
