package org.geode.scaling.app.cluster;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.geode.scaling.app.EventBusRegionFactory;
import org.geode.scaling.cache.model.GlobalStateKey;
import org.geode.scaling.cache.model.PartitionOwnershipRegionValue;
import org.scaling.app.model.PartitionKeyOwnershipView;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;


public class GeodeOwnershipRegionListener extends CacheListenerAdapter<GlobalStateKey, PartitionOwnershipRegionValue> implements Declarable, PartitionOwnershipNotifier {
    private Set<PartitionOwnershipListener> partitionOwnershipListeners = new ConcurrentSkipListSet<>();
    private Cache cache;
    private String nodeId;

    public GeodeOwnershipRegionListener(Cache cache, String nodeId) {
        this.cache = cache;
        this.nodeId = nodeId;
    }



    @Override
    public void addListener(PartitionOwnershipListener partitionOwnershipListener) {
        // this can be easily replaced by rxJava
        partitionOwnershipListeners.add(partitionOwnershipListener);
    }


    private void notifyApplicationAboutOwnershipChanges(PartitionOwnershipRegionValue newValue) {
        PartitionKeyOwnershipView partitionKeyOwnershipView = new PartitionKeyOwnershipView(newValue.getPartitionKeyOwnerStateList());
        handleEventBusRegionCreation(partitionKeyOwnershipView);
        this.partitionOwnershipListeners.stream().forEach(listener -> listener.handleOwnershipChange(partitionKeyOwnershipView));
    }

    private void handleEventBusRegionCreation(PartitionKeyOwnershipView partitionKeyOwnershipView) {
        List<String> otherIds = partitionKeyOwnershipView.getPartitionIdToKeyLoadMap().keySet().stream().filter(partitionId -> !this.nodeId.equals(partitionId)).collect(Collectors.toList());
        otherIds.stream()
                .filter(nodeId -> Objects.isNull(cache.getRegion("EventBufferRegion"+nodeId)))
                .forEach(nodeId -> new EventBusRegionFactory().eventBufferRegion(cache, nodeId));
    }

    @Override
    public void afterCreate(EntryEvent<GlobalStateKey, PartitionOwnershipRegionValue> event) {
        super.afterCreate(event);
        notifyApplicationAboutOwnershipChanges(event.getNewValue());
    }

    @Override
    public void afterDestroy(EntryEvent<GlobalStateKey, PartitionOwnershipRegionValue> event) {
        super.afterDestroy(event);
        notifyApplicationAboutOwnershipChanges(event.getNewValue());
    }

    @Override
    public void afterInvalidate(EntryEvent<GlobalStateKey, PartitionOwnershipRegionValue> event) {
        super.afterInvalidate(event);
        notifyApplicationAboutOwnershipChanges(event.getNewValue());
    }

    @Override
    public void afterUpdate(EntryEvent<GlobalStateKey, PartitionOwnershipRegionValue> event) {
        super.afterUpdate(event);
        notifyApplicationAboutOwnershipChanges(event.getNewValue());
    }


}
