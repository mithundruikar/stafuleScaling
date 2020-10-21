package org.geode.scaling.app;

import org.geode.scaling.app.cluster.PartitionOwnershipRegion;
import org.scaling.app.model.PartitionKeyOwnerState;
import org.scaling.app.model.PartitionKeyOwnershipView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GeodeNodeStartup implements ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeodeNodeStartup.class);

    private String nodeId;
    private PartitionOwnershipRegion clusterPartitionStoreBridge;

    public GeodeNodeStartup(String nodeId, PartitionOwnershipRegion clusterPartitionStoreBridge) {
        this.nodeId = nodeId;
        this.clusterPartitionStoreBridge = clusterPartitionStoreBridge;
    }

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        PartitionKeyOwnershipView partitionKeyOwnershipView = this.clusterPartitionStoreBridge.getPartitionKeyOwnershipView();
        PartitionKeyOwnershipView newPartitionKeyOwnershipView = null;
        do {
            List<PartitionKeyOwnerState> collect = partitionKeyOwnershipView.getPartitionKeyOwnerStates().stream().filter(state -> !state.getPartitionId().equals(this.nodeId)).collect(Collectors.toList());
            PartitionKeyOwnerState partitionKeyOwnerState = new PartitionKeyOwnerState(this.nodeId, new TreeSet<>());
            SortedSet<PartitionKeyOwnerState> updatedStates = new TreeSet<>(collect);
            updatedStates.add(partitionKeyOwnerState);
            newPartitionKeyOwnershipView = new PartitionKeyOwnershipView(updatedStates);
            partitionKeyOwnershipView = this.clusterPartitionStoreBridge.updateOwnershipViewAtomically(newPartitionKeyOwnershipView, partitionKeyOwnershipView);
            LOGGER.info("Registered on start up partition id {}", this.nodeId);
        } while(!partitionKeyOwnershipView.equals(newPartitionKeyOwnershipView));
    }

}
