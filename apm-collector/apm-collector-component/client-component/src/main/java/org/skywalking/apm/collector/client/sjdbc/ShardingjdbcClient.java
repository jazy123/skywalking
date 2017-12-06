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

import io.shardingjdbc.core.api.ShardingDataSourceFactory;
import io.shardingjdbc.core.api.config.ShardingRuleConfiguration;
import io.shardingjdbc.core.api.config.TableRuleConfiguration;
import io.shardingjdbc.core.api.config.strategy.InlineShardingStrategyConfiguration;
import org.skywalking.apm.collector.client.Client;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author linjiaqi, wangkai
 */
public class ShardingjdbcClient implements Client {

    private final Logger logger = LoggerFactory.getLogger(ShardingjdbcClient.class);

    private List<ShardingNode> nodes;
    private DataSource dataSource;

    private static TableRuleConfiguration getOrderTableRuleConfiguration() {
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration();
        orderTableRuleConfig.setLogicTable("t_order");
        orderTableRuleConfig.setKeyGeneratorColumnName("order_id");
        return orderTableRuleConfig;
    }

    private static TableRuleConfiguration getOrderItemTableRuleConfiguration() {
        TableRuleConfiguration orderItemTableRuleConfig = new TableRuleConfiguration();
        orderItemTableRuleConfig.setLogicTable("t_order_item");
        return orderItemTableRuleConfig;
    }


    public ShardingjdbcClient(List<ShardingNode> nodes) {
        this.nodes = nodes;
    }

    @Override
    public void initialize() throws ShardingjdbcClientException {
        try {
            Map<String, DataSource> result = new HashMap<>(nodes.size(), 1);
            for (int i = 0; i < nodes.size(); i++) {
                BasicDataSource dataSource0 = new BasicDataSource();
                dataSource0.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
                dataSource0.setUrl(nodes.get(i).getUrl() + i);
                dataSource0.setUsername(nodes.get(i).getUsername());
                dataSource0.setPassword(nodes.get(i).getPassword());
                result.put("skywalking_ds_" + i, dataSource);
            }
            ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
            shardingRuleConfig.getTableRuleConfigs().add(getOrderTableRuleConfiguration());
            shardingRuleConfig.getTableRuleConfigs().add(getOrderItemTableRuleConfiguration());
            shardingRuleConfig.setDefaultDatabaseShardingStrategyConfig(new InlineShardingStrategyConfiguration("user_id", "demo_ds_${user_id % 2}"));
            dataSource = ShardingDataSourceFactory.createDataSource(result, shardingRuleConfig, new HashMap<String, Object>(), new Properties());
        } catch (Exception e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        try {
            dataSource.getConnection().close();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public Connection getConnection() throws ShardingjdbcClientException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage());
        }
    }

    public void execute(String sql) throws ShardingjdbcClientException {
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(sql);
            statement.closeOnCompletion();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
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
            statement.closeOnCompletion();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
        return rs;
    }

    public boolean execute(String sql, Object[] params) throws ShardingjdbcClientException {
        logger.debug("execute insert/update/delete: {}", sql);
        boolean flag;
        Connection conn = getConnection();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            conn.setAutoCommit(true);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            flag = statement.execute();
            statement.closeOnCompletion();
        } catch (SQLException e) {
            throw new ShardingjdbcClientException(e.getMessage(), e);
        }
        return flag;
    }
}
