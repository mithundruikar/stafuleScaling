package org.geode.scaling.app.dispatcher;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.entry.EventDispatcher;
import org.geode.scaling.app.cluster.ClusterPartitioningLogic;
import org.scaling.app.model.Event;
import org.scaling.app.model.EventKey;

public class GeodeEventDispatcher implements EventDispatcher {
    private Region<EventKey, Event> eventBackupRegion;
    private Cache cache;
    private ClusterPartitioningLogic clusterPartitioningLogic;

    public GeodeEventDispatcher(Region<EventKey, Event> eventBackupRegion, Cache cache, ClusterPartitioningLogic clusterPartitioningLogic) {
        this.eventBackupRegion = eventBackupRegion;
        this.cache = cache;
        this.clusterPartitioningLogic = clusterPartitioningLogic;
    }

    @Override
    public boolean handleEvent(Event event) {
        eventBackupRegion.put(event.getEventKey(), event);
        String partitionId = clusterPartitioningLogic.getPartitionId(event.getPartitionIdField());
        Region<EventKey, Event> eventBusRegion = this.cache.getRegion("EventBufferRegion" + partitionId);
        eventBusRegion.put(event.getEventKey(), event);
        return true;
    }
}
