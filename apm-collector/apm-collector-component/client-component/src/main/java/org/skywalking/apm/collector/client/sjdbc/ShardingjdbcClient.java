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

package org.skywalking.apm.collector.client.sjdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.skywalking.apm.collector.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.shardingjdbc.core.api.ShardingDataSourceFactory;
import io.shardingjdbc.core.api.config.ShardingRuleConfiguration;

/**
 * @author linjiaqi, wangkai
 */
public class ShardingjdbcClient implements Client {

    private final Logger logger = LoggerFactory.getLogger(ShardingjdbcClient.class);

    private Map<String, ShardingNode> shardingNodes;

    private ShardingRuleConfiguration shardingRuleConfig;

    private Map<String, DataSource> shardingDataSource = new HashMap<String, DataSource>();

    private DataSource dataSource;

    public ShardingjdbcClient(Map<String, ShardingNode> shardingNodes, ShardingRuleConfiguration shardingRuleConfig) {
        this.shardingNodes = shardingNodes;
        this.shardingRuleConfig = shardingRuleConfig;
    }

    @Override
    public void initialize() throws ShardingjdbcClientException {
        try {
            shardingNodes.forEach((key, shardingNode) -> {
                BasicDataSource basicDataSource = new BasicDataSource();
                basicDataSource.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
                basicDataSource.setUrl(shardingNode.getUrl());
                basicDataSource.setUsername(shardingNode.getUsername());
                basicDataSource.setPassword(shardingNode.getPassword());
                shardingDataSource.put(key, basicDataSource);
            });
            dataSource = ShardingDataSourceFactory.createDataSource(shardingDataSource, shardingRuleConfig,
                    new HashMap<String, Object>(), new Properties());
        } catch (Exception e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        
    }

    public Connection getConnection() throws ShardingjdbcClientException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage());
        }
    }

    public void execute(String sql) throws ShardingjdbcClientException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConnection();
            statement = conn.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new ShardingjdbcClientException(e.getMessage(), e);
            }
        }
    }

    public ResultSet executeQuery(String sql, Object[] params) throws ShardingjdbcClientException {
        logger.debug("execute query with result: {}", sql);
        ResultSet rs;
        PreparedStatement statement;
        try {
            statement = getConnection().prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            rs = statement.executeQuery();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
        return rs;
    }

    public boolean execute(String sql, Object[] params) throws ShardingjdbcClientException {
        logger.debug("execute insert/update/delete: {}", sql);
        boolean flag;
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(true);
            statement = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            flag = statement.execute();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new ShardingjdbcClientException(e.getMessage(), e);
            }
        }
        return flag;
    }
}
