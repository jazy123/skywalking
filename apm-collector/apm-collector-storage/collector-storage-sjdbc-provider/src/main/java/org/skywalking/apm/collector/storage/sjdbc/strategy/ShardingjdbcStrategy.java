package org.skywalking.apm.collector.storage.sjdbc.strategy;

import java.util.ArrayList;
import java.util.List;

import org.skywalking.apm.collector.core.data.CommonTable;
import org.skywalking.apm.collector.storage.table.global.GlobalTraceTable;
import org.skywalking.apm.collector.storage.table.instance.InstPerformanceTable;
import org.skywalking.apm.collector.storage.table.jvm.CpuMetricTable;
import org.skywalking.apm.collector.storage.table.jvm.GCMetricTable;
import org.skywalking.apm.collector.storage.table.jvm.MemoryMetricTable;
import org.skywalking.apm.collector.storage.table.jvm.MemoryPoolMetricTable;
import org.skywalking.apm.collector.storage.table.node.NodeComponentTable;
import org.skywalking.apm.collector.storage.table.node.NodeMappingTable;
import org.skywalking.apm.collector.storage.table.noderef.NodeReferenceTable;
import org.skywalking.apm.collector.storage.table.register.ApplicationTable;
import org.skywalking.apm.collector.storage.table.register.InstanceTable;
import org.skywalking.apm.collector.storage.table.register.ServiceNameTable;
import org.skywalking.apm.collector.storage.table.segment.SegmentCostTable;
import org.skywalking.apm.collector.storage.table.segment.SegmentTable;
import org.skywalking.apm.collector.storage.table.service.ServiceEntryTable;
import org.skywalking.apm.collector.storage.table.serviceref.ServiceReferenceTable;

import io.shardingjdbc.core.api.config.TableRuleConfiguration;
import io.shardingjdbc.core.api.config.strategy.InlineShardingStrategyConfiguration;
import io.shardingjdbc.core.api.config.strategy.ShardingStrategyConfiguration;

public class ShardingjdbcStrategy {
    
    public final static String SHARDING_DS_PREFIX = "skywalking_ds_";
    
    private int shardingNodeSize;
    
    private String actualDataNodesPrefix;
    
    private String strategyConfigPrefix;
    
    private List<TableRuleConfiguration> tableRules = new ArrayList<TableRuleConfiguration>();
    
    public ShardingjdbcStrategy(int shardingNodeSize) {
        this.shardingNodeSize = shardingNodeSize;
        this.actualDataNodesPrefix = SHARDING_DS_PREFIX + "${0.." + (shardingNodeSize - 1) + "}.";
        this.strategyConfigPrefix = SHARDING_DS_PREFIX + "${";
        
        tableRules.add(tableRule(ApplicationTable.TABLE, ApplicationTable.COLUMN_APPLICATION_ID));
        tableRules.add(tableRule(InstanceTable.TABLE, InstanceTable.COLUMN_INSTANCE_ID));
        tableRules.add(tableRule(InstPerformanceTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(CpuMetricTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(GCMetricTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(MemoryMetricTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(MemoryPoolMetricTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(GlobalTraceTable.TABLE, GlobalTraceTable.COLUMN_SEGMENT_ID));
        tableRules.add(tableRule(SegmentTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(SegmentCostTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(NodeComponentTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(NodeMappingTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(NodeReferenceTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(ServiceEntryTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(ServiceNameTable.TABLE, CommonTable.COLUMN_ID));
        tableRules.add(tableRule(ServiceReferenceTable.TABLE, CommonTable.COLUMN_ID));
    }

    private TableRuleConfiguration tableRule(String tableName, String columnName) {
        TableRuleConfiguration tableRuleConfiguration = new TableRuleConfiguration();
        tableRuleConfiguration.setLogicTable(tableName);
        tableRuleConfiguration.setActualDataNodes(actualDataNodesPrefix + tableName);
        ShardingStrategyConfiguration configuration = new InlineShardingStrategyConfiguration(columnName, strategyConfigPrefix + columnName + " % " + shardingNodeSize + "}");
        tableRuleConfiguration.setDatabaseShardingStrategyConfig(configuration);
        return tableRuleConfiguration;
    }
    
    public List<TableRuleConfiguration> tableRules() {
        return tableRules;
    }
    
    public ShardingStrategyConfiguration defaultDatabaseSharding() {
        return new InlineShardingStrategyConfiguration(CommonTable.COLUMN_ID, strategyConfigPrefix + CommonTable.COLUMN_ID + " % " + shardingNodeSize + "}");
    }
}
