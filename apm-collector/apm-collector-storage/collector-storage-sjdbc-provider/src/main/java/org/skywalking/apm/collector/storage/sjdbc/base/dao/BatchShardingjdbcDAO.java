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

package org.skywalking.apm.collector.storage.sjdbc.base.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.storage.base.dao.IBatchDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class BatchShardingjdbcDAO extends ShardingjdbcDAO implements IBatchDAO {

    private final Logger logger = LoggerFactory.getLogger(BatchShardingjdbcDAO.class);

    public BatchShardingjdbcDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override
    public void batchPersistence(List<?> batchCollection) {
        if (batchCollection != null && batchCollection.size() > 0) {
            logger.debug("the batch collection size is {}", batchCollection.size());
            Connection conn = null;
            final Map<String, PreparedStatement> batchSqls = new HashMap<>();
            try {
                conn = getClient().getConnection();
                conn.setAutoCommit(true);
                PreparedStatement ps = null;
                for (Object entity : batchCollection) {
                    ShardingjdbcSqlEntity e = getShardingjdbcSqlEntity(entity);
                    String sql = e.getSql();
                    if (batchSqls.containsKey(sql)) {
                        ps = batchSqls.get(sql);
                    } else {
                        ps = conn.prepareStatement(sql);
                        batchSqls.put(sql, ps);
                    }

                    Object[] params = e.getParams();
                    if (params != null) {
                        logger.debug("the sql is {}, params size is {}, params: {}", e.getSql(), params.length, params);
                        for (int i = 0; i < params.length; i++) {
                            ps.setObject(i + 1, params[i]);
                        }
                    }
                    ps.addBatch();
                }

                for (String k : batchSqls.keySet()) {
                    batchSqls.get(k).executeBatch();
                }
            } catch (SQLException | ShardingjdbcClientException e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    for (PreparedStatement ps : batchSqls.values()) {
                        if (ps != null) {
                            ps.close();
                        }
                    }
                    if (conn != null) {
                        conn.close();
                    }
                    batchSqls.clear();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
                
            }
        }
    }

    private ShardingjdbcSqlEntity getShardingjdbcSqlEntity(Object entity) {
        if (entity instanceof ShardingjdbcSqlEntity) {
            return (ShardingjdbcSqlEntity)entity;
        }
        return null;
    }
}
