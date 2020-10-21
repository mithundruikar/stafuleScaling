package org.geode.scaling.app.cluster;

public interface PartitionOwnershipNotifier {
    void addListener(PartitionOwnershipListener partitionOwnershipListener);
}
