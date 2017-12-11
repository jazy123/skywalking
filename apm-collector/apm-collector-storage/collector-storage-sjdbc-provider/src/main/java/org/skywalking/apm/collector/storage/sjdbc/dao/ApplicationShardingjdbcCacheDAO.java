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
import org.skywalking.apm.collector.core.util.Const;
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.IApplicationCacheDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.register.ApplicationTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class ApplicationShardingjdbcCacheDAO extends ShardingjdbcDAO implements IApplicationCacheDAO {

    private final Logger logger = LoggerFactory.getLogger(ApplicationShardingjdbcCacheDAO.class);
    private static final String GET_APPLICATION_ID_OR_CODE_SQL = "select {0} from {1} where {2} = ?";

    public ApplicationShardingjdbcCacheDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override
    public int getApplicationId(String applicationCode) {
        logger.info("get the application getId with application code = {}", applicationCode);
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_APPLICATION_ID_OR_CODE_SQL, ApplicationTable.COLUMN_APPLICATION_ID, ApplicationTable.TABLE, ApplicationTable.COLUMN_APPLICATION_CODE);

        Object[] params = new Object[] {applicationCode};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }

    @Override public String getApplicationCode(int applicationId) {
        logger.debug("get application code, applicationId: {}", applicationId);
        ShardingjdbcClient client = getClient();
        String sql = SqlBuilder.buildSql(GET_APPLICATION_ID_OR_CODE_SQL, ApplicationTable.COLUMN_APPLICATION_CODE, ApplicationTable.TABLE, ApplicationTable.COLUMN_APPLICATION_ID);
        Object[] params = new Object[] {applicationId};
        try (
                ResultSet rs = client.executeQuery(sql, params);
                Statement st = rs.getStatement();
                Connection conn = st.getConnection();
            ) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
        return Const.EMPTY_STRING;
    }
}
