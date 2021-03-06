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
import org.skywalking.apm.collector.storage.base.sql.SqlBuilder;
import org.skywalking.apm.collector.storage.dao.ISegmentPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.ShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcSqlEntity;
import org.skywalking.apm.collector.storage.table.segment.Segment;
import org.skywalking.apm.collector.storage.table.segment.SegmentTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class SegmentShardingjdbcPersistenceDAO extends ShardingjdbcDAO implements ISegmentPersistenceDAO<ShardingjdbcSqlEntity, ShardingjdbcSqlEntity, Segment> {

    private final Logger logger = LoggerFactory.getLogger(SegmentShardingjdbcPersistenceDAO.class);

    public SegmentShardingjdbcPersistenceDAO(ShardingjdbcClient client) {
        super(client);
    }

    @Override public Segment get(String id) {
        return null;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchInsert(Segment data) {
        Map<String, Object> source = new HashMap<>();
        ShardingjdbcSqlEntity entity = new ShardingjdbcSqlEntity();
        source.put(SegmentTable.COLUMN_ID, data.getId());
        source.put(SegmentTable.COLUMN_DATA_BINARY, data.getDataBinary());
        source.put(SegmentTable.COLUMN_TIME_BUCKET, data.getTimeBucket());
        logger.debug("segment source: {}", source.toString());

        String sql = SqlBuilder.buildBatchInsertSql(SegmentTable.TABLE, source.keySet());
        entity.setSql(sql);
        entity.setParams(source.values().toArray(new Object[0]));
        return entity;
    }

    @Override public ShardingjdbcSqlEntity prepareBatchUpdate(Segment data) {
        return null;
    }
}
