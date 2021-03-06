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

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.IInstanceCacheDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.register.InstanceTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class InstanceShardingjdbcCacheDAO extends ShardingjdbcDAO implements IInstanceCacheDAO {

    private final Logger logger = LoggerFactory.getLogger(InstanceShardingjdbcCacheDAO.class);

    private static final String GET_APPLICATION_ID_SQL = "select {0} from {1} where {2} = ?";
    private static final String GET_INSTANCE_ID_SQL = "select {0} from {1} where {2} = ? and {3} = ?";

    public InstanceShardingjdbcCacheDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public int getApplicationId(int instanceId) {
        logger.info("get the application getId with application getId = {}", instanceId);
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_APPLICATION_ID_SQL, InstanceTable.COLUMN_APPLICATION_ID, InstanceTable.TABLE, InstanceTable.COLUMN_INSTANCE_ID);
        Object[] params = new Object[] {instanceId};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                return rs.getInt(InstanceTable.COLUMN_APPLICATION_ID);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }

    @Override public int getInstanceId(int applicationId, String agentUUID) {
        logger.info("get the application getId with application getId = {}, agentUUID = {}", applicationId, agentUUID);
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_INSTANCE_ID_SQL, InstanceTable.COLUMN_INSTANCE_ID, InstanceTable.TABLE, InstanceTable.COLUMN_APPLICATION_ID,
            InstanceTable.COLUMN_AGENT_UUID);
        Object[] params = new Object[] {applicationId, agentUUID};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                return rs.getInt(InstanceTable.COLUMN_INSTANCE_ID);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }
}
