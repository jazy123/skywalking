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
import org.skywalking.apm.collector.storage.dao.INodeMappingUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.node.NodeMappingTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * @author linjiaqi
 */
public class NodeMappingShardingjdbcUIDAO extends ShardingjdbcDAO implements INodeMappingUIDAO {

    private final Logger logger = LoggerFactory.getLogger(NodeMappingShardingjdbcUIDAO.class);
    private static final String NODE_MAPPING_SQL = "select {0}, {1} from {2} where {3} >= ? and {3} <= ? group by {0}, {1} limit 100";

    public NodeMappingShardingjdbcUIDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public JsonArray load(long startTime, long endTime) {
        ShardingjdbcClient client = getClient();
        JsonArray nodeMappingArray = new JsonArray();
        String sql = SqlBuilder.buildSql(NODE_MAPPING_SQL, NodeMappingTable.COLUMN_APPLICATION_ID,
            NodeMappingTable.COLUMN_ADDRESS_ID, NodeMappingTable.TABLE, NodeMappingTable.COLUMN_TIME_BUCKET);

        Object[] params = new Object[] {startTime, endTime};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            while (rs.next()) {
                int applicationId = rs.getInt(NodeMappingTable.COLUMN_APPLICATION_ID);
                int addressId = rs.getInt(NodeMappingTable.COLUMN_ADDRESS_ID);
                JsonObject nodeMappingObj = new JsonObject();
                nodeMappingObj.addProperty(NodeMappingTable.COLUMN_APPLICATION_ID, applicationId);
                nodeMappingObj.addProperty(NodeMappingTable.COLUMN_ADDRESS_ID, addressId);
                nodeMappingArray.add(nodeMappingObj);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        logger.debug("node mapping data: {}", nodeMappingArray.toString());
        return nodeMappingArray;
    }
}
