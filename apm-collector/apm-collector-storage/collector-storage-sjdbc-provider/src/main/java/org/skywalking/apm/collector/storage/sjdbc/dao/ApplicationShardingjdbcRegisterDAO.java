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
import org.skywalking.apm.collector.storage.dao.IApplicationRegisterDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.table.register.Application;
import org.skywalking.apm.collector.storage.table.register.ApplicationTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class ApplicationShardingjdbcRegisterDAO extends ShardingjdbcDAO implements IApplicationRegisterDAO {
    private final Logger logger = LoggerFactory.getLogger(ApplicationShardingjdbcRegisterDAO.class);

    public ApplicationShardingjdbcRegisterDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override
    public int getMaxApplicationId() {
        return getMaxId(ApplicationTable.TABLE, ApplicationTable.COLUMN_APPLICATION_ID);
    }

    @Override
    public int getMinApplicationId() {
        return getMinId(ApplicationTable.TABLE, ApplicationTable.COLUMN_APPLICATION_ID);
    }

    @Override
    public void save(Application application) {
        ShardingjdbcClient client = getClient();

        Map<String, Object> source = new HashMap<>();
        source.put(ApplicationTable.COLUMN_ID, application.getId());
        source.put(ApplicationTable.COLUMN_APPLICATION_CODE, application.getApplicationCode());
        source.put(ApplicationTable.COLUMN_APPLICATION_ID, application.getApplicationId());

        String sql = SqlBuilder.buildBatchInsertSql(ApplicationTable.TABLE, source.keySet());
        Object[] params = source.values().toArray(new Object[0]);
        try {
            client.execute(sql, params);
        } catch (ShardingjdbcClientException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
