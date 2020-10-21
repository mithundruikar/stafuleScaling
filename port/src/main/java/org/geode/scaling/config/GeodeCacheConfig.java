package org.geode.scaling.config;

import org.apache.geode.cache.*;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.geode.scaling.app.*;
import org.geode.scaling.app.cluster.*;
import org.geode.scaling.app.dispatcher.GeodeEventDispatcher;
import org.scaling.app.cluster.ClusterPartitionBusinessLogic;
import org.scaling.app.config.ServiceConfig;
import org.scaling.app.model.Event;
import org.scaling.app.model.EventKey;
import org.geode.scaling.cache.model.GlobalStateKey;
import org.geode.scaling.cache.model.PartitionOwnershipRegionValue;
import org.scaling.app.service.EventConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Properties;

@Configuration
@Import({ServiceConfig.class})
public class GeodeCacheConfig {

    @Bean
    public Cache cache(Properties sysProperties) {
        Properties geodeProps = new Properties();
        String serverName = sysProperties.getProperty("serverName");
        geodeProps.setProperty("name", serverName);
        geodeProps.setProperty("locators", "localhost[13489]");
        if("node1".equals(serverName)) {
            geodeProps.setProperty("start-locator", "localhost[13489]");
        }
        CacheFactory cacheFactory = new CacheFactory(geodeProps);
        cacheFactory.set("cache-xml-file", "cache.xml");
        GemFireCacheImpl.setDefaultDiskStoreName("DefaultDiskStore"+serverName);
        return cacheFactory.create();
    }

    @Bean
    public AsyncEventQueue asyncEventQueueFactory(Cache cache, Properties sysProperties, GeodeNodeEventsListener geodeNodeEventsListener) {
        String serverName = sysProperties.getProperty("serverName");

        AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
        factory.setBatchSize(10000);
        factory.setBatchTimeInterval(10000);  // this is in milli second
        factory.setMaximumQueueMemory(10000); // this is in MB
        factory.setPersistent(false);
        factory.setParallel(false);
        factory.setOrderPolicy(GatewaySender.OrderPolicy.PARTITION);
        factory.setDiskSynchronous(false);

        return factory.create("asyncEventQueue" + serverName, geodeNodeEventsListener);
    }

    @Bean
    public Region<GlobalStateKey, PartitionOwnershipRegionValue> partitionOwnershipGeodeRegion(Cache cache, GeodeOwnershipRegionListener geodeOwnershipRegionListener) {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);

        RegionFactory<GlobalStateKey, PartitionOwnershipRegionValue> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
        rf.setPartitionAttributes(paf.create());
        rf.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        rf.addCacheListener(geodeOwnershipRegionListener);
        Region<GlobalStateKey, PartitionOwnershipRegionValue> partitionOwnershipRegion = rf.create("PartitionOwnershipRegion");

        return partitionOwnershipRegion;
    }


    @Bean
    public Region<EventKey, Event> eventBufferRegion(Cache cache, Properties sysProperties) {
        String serverName = sysProperties.getProperty("serverName");
        return new EventBusRegionFactory().eventBufferRegion(cache, serverName);
    }

    @Bean
    public GeodeNodeEventsListener gemfireNodeEventsListener(Cache cache, Properties sysProperties, EventConsumer eventConsumer) {
        String serverName = sysProperties.getProperty("serverName");
        return new GeodeNodeEventsListener(serverName, cache, eventConsumer);
    }


    @Bean
    public Region<EventKey, Event> eventBackupRegion(Cache cache) {
        return cache.getRegion("EventBackupRegion");
    }

    @Bean
    public GeodeOwnershipRegionListener geodeOwnershipRegionListener(Cache cache, Properties sysProperties) {
        String serverName = sysProperties.getProperty("serverName");
        return new GeodeOwnershipRegionListener(cache, serverName);
    }

    @Bean
    public PartitionOwnershipRegion partitionOwnershipRegion(Region<GlobalStateKey, PartitionOwnershipRegionValue> ownershipRegionValueRegion,
                                                             ClusterPartitionBusinessLogic clusterPartitionBusinessLogic) {
        return new PartitionOwnershipRegion(ownershipRegionValueRegion, clusterPartitionBusinessLogic);
    }

    @Bean
    public GeodeMembershipListener geodeMembershipListener(Cache cache, PartitionOwnershipRegion partitionOwnershipRegion) {
        return new GeodeMembershipListener(cache, partitionOwnershipRegion);
    }

    @Bean
    public GeodeNodeStartup geodeNodeStartup(Properties sysProperties, PartitionOwnershipRegion partitionOwnershipRegion) {
        String serverName = sysProperties.getProperty("serverName");
        return new GeodeNodeStartup(serverName, partitionOwnershipRegion);
    }

    @Bean
    public ClusterPartitioningLogic clusterPartitioningLogic(ClusterPartitionBusinessLogic clusterPartitionBusinessLogic, PartitionOwnershipRegion clusterPartitionStoreBridge,
                                                             GeodeOwnershipRegionListener geodeOwnershipRegionListener) {
        return new ClusterPartitioningLogic(clusterPartitionBusinessLogic, geodeOwnershipRegionListener, clusterPartitionStoreBridge);
    }

    @Bean
    public GeodeEventDispatcher geodeEventDispatcher(Region<EventKey, Event> eventBackupRegion, Cache cache, ClusterPartitioningLogic clusterPartitioningLogic) {
        return new GeodeEventDispatcher(eventBackupRegion, cache, clusterPartitioningLogic);
    }
}
