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
package org.skywalking.apm.collector.storage.sjdbc;

import org.skywalking.apm.collector.client.sjdbc.ShardingNode;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.core.module.Module;
import org.skywalking.apm.collector.core.module.ModuleProvider;
import org.skywalking.apm.collector.core.module.ServiceNotProvidedException;
import org.skywalking.apm.collector.storage.StorageException;
import org.skywalking.apm.collector.storage.StorageModule;
import org.skywalking.apm.collector.storage.base.dao.IBatchDAO;
import org.skywalking.apm.collector.storage.dao.*;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.BatchShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcStorageInstaller;
import org.skywalking.apm.collector.storage.sjdbc.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author wangkai
 */
public class StorageModuleShardingjdbcProvider extends ModuleProvider {

    private final Logger logger = LoggerFactory.getLogger(StorageModuleShardingjdbcProvider.class);

    private static final String URL = "url";
    private static final String USER_NAME = "username";
    private static final String PASSWORD = "password";

    private ShardingjdbcClient shardingjdbcClient;

    @Override
    public String name() {
        return "shardingjdbc";
    }

    @Override
    public Class<? extends Module> module() {
        return StorageModule.class;
    }

    @Override
    public void prepare(Properties config) throws ServiceNotProvidedException {
        List<ShardingNode> nodes = new ArrayList<>();
        int index = 0;
        while (true) {
            String url = config.getProperty(URL + "_" + index);
            String username = config.getProperty(USER_NAME + "_" + index);
            String password = config.getProperty(PASSWORD + "_" + index);
            if (url != null && username != null && password != null) {
                nodes.add(new ShardingNode(url, username, password));
                index++;
            } else {
                break;
            }
        }
        shardingjdbcClient = new ShardingjdbcClient(nodes);
        this.registerServiceImplementation(IBatchDAO.class, new BatchShardingjdbcDAO(shardingjdbcClient));
        registerCacheDAO();
        registerRegisterDAO();
        registerPersistenceDAO();
        registerUiDAO();
    }

    @Override
    public void start(Properties config) throws ServiceNotProvidedException {
        try {
            shardingjdbcClient.initialize();

            ShardingjdbcStorageInstaller installer = new ShardingjdbcStorageInstaller();
            installer.install(shardingjdbcClient);
        } catch (ShardingjdbcClientException | StorageException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void notifyAfterCompleted() throws ServiceNotProvidedException {

    }

    @Override
    public String[] requiredModules() {
        return new String[0];
    }

    private void registerCacheDAO() throws ServiceNotProvidedException {
        this.registerServiceImplementation(IApplicationCacheDAO.class, new ApplicationShardingjdbcCacheDAO(shardingjdbcClient));
        this.registerServiceImplementation(IInstanceCacheDAO.class, new InstanceShardingjdbcCacheDAO(shardingjdbcClient));
        this.registerServiceImplementation(IServiceNameCacheDAO.class, new ServiceNameShardingjdbcCacheDAO(shardingjdbcClient));
    }

    private void registerRegisterDAO() throws ServiceNotProvidedException {
        this.registerServiceImplementation(IApplicationRegisterDAO.class, new ApplicationShardingjdbcRegisterDAO(shardingjdbcClient));
        this.registerServiceImplementation(IInstanceRegisterDAO.class, new InstanceShardingjdbcRegisterDAO(shardingjdbcClient));
        this.registerServiceImplementation(IServiceNameRegisterDAO.class, new ServiceNameShardingjdbcRegisterDAO(shardingjdbcClient));
    }

    private void registerPersistenceDAO() throws ServiceNotProvidedException {
        this.registerServiceImplementation(ICpuMetricPersistenceDAO.class, new CpuMetricShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(IGCMetricPersistenceDAO.class, new GCMetricShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(IMemoryMetricPersistenceDAO.class, new MemoryMetricShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(IMemoryPoolMetricPersistenceDAO.class, new MemoryPoolMetricShardingjdbcPersistenceDAO(shardingjdbcClient));

        this.registerServiceImplementation(IGlobalTracePersistenceDAO.class, new GlobalTraceShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(IInstPerformancePersistenceDAO.class, new InstPerformanceShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(INodeComponentPersistenceDAO.class, new NodeComponentShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(INodeMappingPersistenceDAO.class, new NodeMappingShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(INodeReferencePersistenceDAO.class, new NodeReferenceShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(ISegmentCostPersistenceDAO.class, new SegmentCostShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(ISegmentPersistenceDAO.class, new SegmentShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(IServiceEntryPersistenceDAO.class, new ServiceEntryShardingjdbcPersistenceDAO(shardingjdbcClient));
        this.registerServiceImplementation(IServiceReferencePersistenceDAO.class, new ServiceReferenceShardingjdbcPersistenceDAO(shardingjdbcClient));

        this.registerServiceImplementation(IInstanceHeartBeatPersistenceDAO.class, new InstanceHeartBeatShardingjdbcPersistenceDAO(shardingjdbcClient));
    }

    private void registerUiDAO() throws ServiceNotProvidedException {
        this.registerServiceImplementation(IInstanceUIDAO.class, new InstanceShardingjdbcUIDAO(shardingjdbcClient));

        this.registerServiceImplementation(ICpuMetricUIDAO.class, new CpuMetricShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(IGCMetricUIDAO.class, new GCMetricShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(IMemoryMetricUIDAO.class, new MemoryMetricShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(IMemoryPoolMetricUIDAO.class, new MemoryPoolMetricShardingjdbcUIDAO(shardingjdbcClient));

        this.registerServiceImplementation(IGlobalTraceUIDAO.class, new GlobalTraceShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(IInstPerformanceUIDAO.class, new InstPerformanceShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(INodeComponentUIDAO.class, new NodeComponentShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(INodeMappingUIDAO.class, new NodeMappingShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(INodeReferenceUIDAO.class, new NodeReferenceShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(ISegmentCostUIDAO.class, new SegmentCostShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(ISegmentUIDAO.class, new SegmentShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(IServiceEntryUIDAO.class, new ServiceEntryShardingjdbcUIDAO(shardingjdbcClient));
        this.registerServiceImplementation(IServiceReferenceUIDAO.class, new ServiceReferenceShardingjdbcUIDAO(shardingjdbcClient));
    }
}
