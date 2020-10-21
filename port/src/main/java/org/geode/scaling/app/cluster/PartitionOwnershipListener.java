package org.geode.scaling.app.cluster;

import org.scaling.app.model.PartitionKeyOwnershipView;

import java.util.List;

public interface PartitionOwnershipListener {
    void handleOwnershipChange(PartitionKeyOwnershipView partitionKeyOwnershipView);
}
