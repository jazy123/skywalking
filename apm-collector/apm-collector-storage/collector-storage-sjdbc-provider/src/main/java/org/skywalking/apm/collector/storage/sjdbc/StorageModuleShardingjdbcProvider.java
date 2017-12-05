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

import java.util.Properties;

import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClient;
import org.skywalking.apm.collector.client.sjdbc.ShardingjdbcClientException;
import org.skywalking.apm.collector.core.module.Module;
import org.skywalking.apm.collector.core.module.ModuleProvider;
import org.skywalking.apm.collector.core.module.ServiceNotProvidedException;
import org.skywalking.apm.collector.storage.StorageException;
import org.skywalking.apm.collector.storage.StorageModule;
import org.skywalking.apm.collector.storage.base.dao.IBatchDAO;
import org.skywalking.apm.collector.storage.dao.IApplicationCacheDAO;
import org.skywalking.apm.collector.storage.dao.IApplicationRegisterDAO;
import org.skywalking.apm.collector.storage.dao.ICpuMetricPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.ICpuMetricUIDAO;
import org.skywalking.apm.collector.storage.dao.IGCMetricPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IGCMetricUIDAO;
import org.skywalking.apm.collector.storage.dao.IGlobalTracePersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IGlobalTraceUIDAO;
import org.skywalking.apm.collector.storage.dao.IInstPerformancePersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IInstPerformanceUIDAO;
import org.skywalking.apm.collector.storage.dao.IInstanceCacheDAO;
import org.skywalking.apm.collector.storage.dao.IInstanceHeartBeatPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IInstanceRegisterDAO;
import org.skywalking.apm.collector.storage.dao.IInstanceUIDAO;
import org.skywalking.apm.collector.storage.dao.IMemoryMetricPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IMemoryMetricUIDAO;
import org.skywalking.apm.collector.storage.dao.IMemoryPoolMetricPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IMemoryPoolMetricUIDAO;
import org.skywalking.apm.collector.storage.dao.INodeComponentPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.INodeComponentUIDAO;
import org.skywalking.apm.collector.storage.dao.INodeMappingPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.INodeMappingUIDAO;
import org.skywalking.apm.collector.storage.dao.INodeReferencePersistenceDAO;
import org.skywalking.apm.collector.storage.dao.INodeReferenceUIDAO;
import org.skywalking.apm.collector.storage.dao.ISegmentCostPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.ISegmentCostUIDAO;
import org.skywalking.apm.collector.storage.dao.ISegmentPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.ISegmentUIDAO;
import org.skywalking.apm.collector.storage.dao.IServiceEntryPersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IServiceEntryUIDAO;
import org.skywalking.apm.collector.storage.dao.IServiceNameCacheDAO;
import org.skywalking.apm.collector.storage.dao.IServiceNameRegisterDAO;
import org.skywalking.apm.collector.storage.dao.IServiceReferencePersistenceDAO;
import org.skywalking.apm.collector.storage.dao.IServiceReferenceUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.dao.BatchShardingjdbcDAO;
import org.skywalking.apm.collector.storage.sjdbc.base.define.ShardingjdbcStorageInstaller;
import org.skywalking.apm.collector.storage.sjdbc.dao.ApplicationShardingjdbcCacheDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ApplicationShardingjdbcRegisterDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.CpuMetricShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.CpuMetricShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.GCMetricShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.GCMetricShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.GlobalTraceShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.GlobalTraceShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.InstPerformanceShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.InstPerformanceShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.InstanceHeartBeatShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.InstanceShardingjdbcCacheDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.InstanceShardingjdbcRegisterDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.InstanceShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.MemoryMetricShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.MemoryMetricShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.MemoryPoolMetricShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.MemoryPoolMetricShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.NodeComponentShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.NodeComponentShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.NodeMappingShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.NodeMappingShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.NodeReferenceShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.NodeReferenceShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.SegmentCostShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.SegmentCostShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.SegmentShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.SegmentShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ServiceEntryShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ServiceEntryShardingjdbcUIDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ServiceNameShardingjdbcCacheDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ServiceNameShardingjdbcRegisterDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ServiceReferenceShardingjdbcPersistenceDAO;
import org.skywalking.apm.collector.storage.sjdbc.dao.ServiceReferenceShardingjdbcUIDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author linjiaqi
 */
public class StorageModuleShardingjdbcProvider extends ModuleProvider {

    private final Logger logger = LoggerFactory.getLogger(StorageModuleShardingjdbcProvider.class);

    private static final String URL = "url";
    private static final String USER_NAME = "user_name";
    private static final String PASSWORD = "password";

    private ShardingjdbcClient shardingjdbcClient;

    @Override public String name() {
        return "shardingjdbc";
    }

    @Override public Class<? extends Module> module() {
        return StorageModule.class;
    }

    @Override public void prepare(Properties config) throws ServiceNotProvidedException {
        String url = config.getProperty(URL);
        String userName = config.getProperty(USER_NAME);
        String password = config.getProperty(PASSWORD);
        shardingjdbcClient = new ShardingjdbcClient(url, userName, password, 2);

        this.registerServiceImplementation(IBatchDAO.class, new BatchShardingjdbcDAO(shardingjdbcClient));
        registerCacheDAO();
        registerRegisterDAO();
        registerPersistenceDAO();
        registerUiDAO();
    }

    @Override public void start(Properties config) throws ServiceNotProvidedException {
        try {
            shardingjdbcClient.initialize();

            ShardingjdbcStorageInstaller installer = new ShardingjdbcStorageInstaller();
            installer.install(shardingjdbcClient);
        } catch (ShardingjdbcClientException | StorageException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override public void notifyAfterCompleted() throws ServiceNotProvidedException {

    }

    @Override public String[] requiredModules() {
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
