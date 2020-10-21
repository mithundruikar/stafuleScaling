package org.geode.scaling.app.cluster;

import org.scaling.app.cluster.ClusterPartitionBusinessLogic;
import org.scaling.app.model.PartitionKeyOwnershipView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterPartitioningLogic implements PartitionOwnershipListener {
    private static Logger LOGGER = LoggerFactory.getLogger(ClusterPartitioningLogic.class);

    private AtomicReference<PartitionKeyOwnershipView> partitionKeyOwnershipViewAtomicReference = new AtomicReference<>(new PartitionKeyOwnershipView(Collections.emptySortedSet()));
    private PartitionOwnershipNotifier partitionOwnershipNotifier;
    private ClusterPartitionBusinessLogic clusterPartitionBusinessLogic;
    private PartitionOwnershipRegion partitionOwnershipRegion;

    public ClusterPartitioningLogic(ClusterPartitionBusinessLogic clusterPartitionBusinessLogic,
                                    PartitionOwnershipNotifier partitionOwnershipNotifier,
                                    PartitionOwnershipRegion partitionOwnershipRegion) {
        this.clusterPartitionBusinessLogic = clusterPartitionBusinessLogic;
        this.partitionOwnershipNotifier = partitionOwnershipNotifier;
        this.partitionOwnershipRegion = partitionOwnershipRegion;
    }

    @PostConstruct
    public void init() {
        this.partitionOwnershipNotifier.addListener(this);
    }


    @Override
    public void handleOwnershipChange(PartitionKeyOwnershipView partitionKeyOwnershipView) {
        partitionKeyOwnershipViewAtomicReference.set(partitionKeyOwnershipView);
    }

    public String getPartitionId(String partitionKey) {
        PartitionKeyOwnershipView partitionKeyOwnershipView = partitionKeyOwnershipViewAtomicReference.get();

        String existingPartitionId = partitionKeyOwnershipView.getPartitionKeyToIdMap().get(partitionKey);
        if(existingPartitionId != null) {
            // happy path scenario where partition mapping is existing
            return existingPartitionId;
        }
        // need to assign partition. this is critical section
        PartitionKeyOwnershipView newView = clusterPartitionBusinessLogic.assignPartition(partitionKey, partitionKeyOwnershipView);
        PartitionKeyOwnershipView currentView = this.partitionOwnershipRegion.updateOwnershipViewAtomically(newView, partitionKeyOwnershipView);
        if(currentView.equals(newView)) {
            // we are good to believe distributed consensus is made
            LOGGER.info("Successfully updated assignment for partition key {} new view {}", partitionKey, currentView.getPartitionIdToKeyLoadMap());
            return newView.getPartitionKeyToIdMap().get(partitionKey);
        } else {
            // recurse. there was a conflict
            LOGGER.info("Retrying Failed to updated assignment for partition key {}. There was update to view. current view {}", partitionKey, currentView.getPartitionIdToKeyLoadMap());
            handleOwnershipChange(currentView);
            return getPartitionId(partitionKey);
        }
    }

}
