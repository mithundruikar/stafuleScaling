package org.geode.scaling.app;

import org.apache.geode.cache.*;
import org.scaling.app.model.Event;
import org.scaling.app.model.EventKey;

public class EventBusRegionFactory {
    public Region<EventKey, Event> eventBufferRegion(Cache cache, String nodeId) {
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(0);

        RegionFactory<EventKey, Event> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
        rf.setPartitionAttributes(paf.create());
        rf.addAsyncEventQueueId("asyncEventQueue"+nodeId);
        return rf.create("EventBufferRegion"+nodeId);
    }
}
