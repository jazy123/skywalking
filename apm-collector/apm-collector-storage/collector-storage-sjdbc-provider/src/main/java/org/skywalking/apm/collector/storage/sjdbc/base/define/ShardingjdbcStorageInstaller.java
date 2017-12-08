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

package org.skywalking.apm.collector.storage.sjdbc.base.define;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.skywalking.apm.collector.client.Client;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.core.data.TableDefine;
import org.skywalking.apm.collector.storage.StorageException;
import org.skywalking.apm.collector.storage.StorageInstallException;
import org.skywalking.apm.collector.storage.StorageInstaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class ShardingjdbcStorageInstaller extends StorageInstaller {

    private final Logger logger = LoggerFactory.getLogger(ShardingjdbcStorageInstaller.class);

    @Override protected void defineFilter(List<TableDefine> tableDefines) {
        int size = tableDefines.size();
        for (int i = size - 1; i >= 0; i--) {
            if (!(tableDefines.get(i) instanceof ShardingjdbcTableDefine)) {
                tableDefines.remove(i);
            }
        }
    }

    @Override protected boolean isExists(Client client, TableDefine tableDefine) throws StorageException {
        ShardingjdbcClient shardingjdbcClient = (ShardingjdbcClient)client;
        Connection conn = null;
        ResultSet rs = null;
        try {
            logger.info("check if table {} exist ", tableDefine.getName());
            conn = shardingjdbcClient.getConnection();
            rs = conn.getMetaData().getTables(null, null, tableDefine.getName().toUpperCase(), null);
            if (rs.next()) {
                return true;
            }
        } catch (SQLException | ShardingjdbcClientException e) {
            throw new StorageInstallException(e.getMessage(), e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new StorageInstallException(e.getMessage(), e);
            }
        }
        return false;
    }

    @Override protected boolean deleteTable(Client client, TableDefine tableDefine) throws StorageException {
        ShardingjdbcClient shardingjdbcClient = (ShardingjdbcClient)client;
        try {
            shardingjdbcClient.execute("drop table if exists " + tableDefine.getName());
            return true;
        } catch (ShardingjdbcClientException e) {
            throw new StorageInstallException(e.getMessage(), e);
        }
    }

    @Override protected boolean createTable(Client client, TableDefine tableDefine) throws StorageException {
        ShardingjdbcClient shardingjdbcClient = (ShardingjdbcClient)client;
        ShardingjdbcTableDefine shardingjdbcTableDefine = (ShardingjdbcTableDefine)tableDefine;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE ").append(shardingjdbcTableDefine.getName()).append(" (");

        shardingjdbcTableDefine.getColumnDefines().forEach(columnDefine -> {
            ShardingjdbcColumnDefine shardingjdbcColumnDefine = (ShardingjdbcColumnDefine)columnDefine;
            if (shardingjdbcColumnDefine.getType().equals(ShardingjdbcColumnDefine.Type.Varchar.name())) {
                sqlBuilder.append(shardingjdbcColumnDefine.getName()).append(" ").append(shardingjdbcColumnDefine.getType()).append("(255),");
            } else {
                sqlBuilder.append(shardingjdbcColumnDefine.getName()).append(" ").append(shardingjdbcColumnDefine.getType()).append(",");
            }
        });
        //remove last comma
        sqlBuilder.delete(sqlBuilder.length() - 1, sqlBuilder.length());
        sqlBuilder.append(")");
        try {
            logger.info("create shardingjdbc table with sql {}", sqlBuilder);
            shardingjdbcClient.execute(sqlBuilder.toString());
        } catch (ShardingjdbcClientException e) {
            throw new StorageInstallException(e.getMessage(), e);
        }
        return true;
    }
}
