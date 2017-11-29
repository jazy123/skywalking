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
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.IInstanceRegisterDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.register.Instance;
import org.skywalking.apm.collector.storage.table.register.InstanceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class InstanceShardingjdbcRegisterDAO extends ShardingjdbcDAO implements IInstanceRegisterDAO {
    
    private final Logger logger = LoggerFactory.getLogger(InstanceShardingjdbcRegisterDAO.class);

    public InstanceShardingjdbcRegisterDAO(ShardingjdbcClient client) {
        super(client);
    }

    private static final String UPDATE_HEARTBEAT_TIME_SQL = "update {0} set {1} = ? where {2} = ?";

    @Override public int getMaxInstanceId() {
        return getMaxId(InstanceTable.TABLE, InstanceTable.COLUMN_INSTANCE_ID);
    }

    @Override public int getMinInstanceId() {
        return getMinId(InstanceTable.TABLE, InstanceTable.COLUMN_INSTANCE_ID);
    }

    @Override public void save(Instance instance) {
    	ShardingjdbcClient client = getClient();
        Map<String, Object> source = new HashMap<>();
        source.put(InstanceTable.COLUMN_ID, instance.getId());
        source.put(InstanceTable.COLUMN_INSTANCE_ID, instance.getInstanceId());
        source.put(InstanceTable.COLUMN_APPLICATION_ID, instance.getApplicationId());
        source.put(InstanceTable.COLUMN_AGENT_UUID, instance.getAgentUUID());
        source.put(InstanceTable.COLUMN_REGISTER_TIME, instance.getRegisterTime());
        source.put(InstanceTable.COLUMN_HEARTBEAT_TIME, instance.getHeartBeatTime());
        source.put(InstanceTable.COLUMN_OS_INFO, instance.getOsInfo());
        String sql = SqlBuilder.buildBatchInsertSql(InstanceTable.TABLE, source.keySet());
        Object[] params = source.values().toArray(new Object[0]);
        try {
            client.execute(sql, params);
        } catch (ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override public void updateHeartbeatTime(int instanceId, long heartbeatTime) {
    	ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(UPDATE_HEARTBEAT_TIME_SQL, InstanceTable.TABLE, InstanceTable.COLUMN_HEARTBEAT_TIME,
            InstanceTable.COLUMN_ID);
        Object[] params = new Object[] {heartbeatTime, instanceId};
        try {
            client.execute(sql, params);
        } catch (ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
