package org.geode.scaling.app.cluster;

import org.apache.geode.cache.Region;
import org.geode.scaling.cache.model.GlobalStateKey;
import org.geode.scaling.cache.model.PartitionOwnershipRegionValue;
import org.scaling.app.cluster.ClusterPartitionBusinessLogic;
import org.scaling.app.model.PartitionKeyOwnershipView;

import java.util.Collections;

public class PartitionOwnershipRegion {

    private Region<GlobalStateKey, PartitionOwnershipRegionValue> keyOwnerStateRegion;
    private ClusterPartitionBusinessLogic clusterPartitionBusinessLogic;

    public PartitionOwnershipRegion(Region<GlobalStateKey, PartitionOwnershipRegionValue> keyOwnerStateRegion, ClusterPartitionBusinessLogic clusterPartitionBusinessLogic) {
        this.keyOwnerStateRegion = keyOwnerStateRegion;
        this.clusterPartitionBusinessLogic = clusterPartitionBusinessLogic;
    }

    public PartitionKeyOwnershipView getPartitionKeyOwnershipView() {
        PartitionOwnershipRegionValue keyOwnerState = keyOwnerStateRegion.get(new GlobalStateKey(GlobalStateKey.GLOBAL_PARTITION_OWNERSHIP_MAP_KEY));
        return new PartitionKeyOwnershipView(keyOwnerState == null ? Collections.emptySortedSet() : keyOwnerState.getPartitionKeyOwnerStateList());
    }

    public PartitionKeyOwnershipView updateOwnershipViewAtomically(PartitionKeyOwnershipView partitionKeyOwnershipView, PartitionKeyOwnershipView lastKnownView) {
        PartitionOwnershipRegionValue lastKnownRegionValue = new PartitionOwnershipRegionValue(GlobalStateKey.GLOBAL_PARTITION_OWNERSHIP_MAP_KEY, lastKnownView.getPartitionKeyOwnerStates());
        PartitionOwnershipRegionValue partitionOwnershipRegionValue = new PartitionOwnershipRegionValue(GlobalStateKey.GLOBAL_PARTITION_OWNERSHIP_MAP_KEY, partitionKeyOwnershipView.getPartitionKeyOwnerStates());
        PartitionOwnershipRegionValue newValue = keyOwnerStateRegion.compute(new GlobalStateKey(GlobalStateKey.GLOBAL_PARTITION_OWNERSHIP_MAP_KEY), (k, v) -> {
            if(v == null || v.equals(lastKnownRegionValue)) {
                return partitionOwnershipRegionValue;
            }
            return v;
        });
        return new PartitionKeyOwnershipView(newValue.getPartitionKeyOwnerStateList());
    }

    public void removePartitionId(String partitionId) {
        PartitionKeyOwnershipView currentView = getPartitionKeyOwnershipView();
        PartitionKeyOwnershipView partitionKeyOwnershipView = this.clusterPartitionBusinessLogic.removePartitionId(partitionId, currentView);
        if(!partitionKeyOwnershipView.equals(updateOwnershipViewAtomically(partitionKeyOwnershipView, currentView)) ) {
            removePartitionId(partitionId);
        }
    }

    public void addPartitionId(String partitionId) {
        PartitionKeyOwnershipView currentView = getPartitionKeyOwnershipView();
        PartitionKeyOwnershipView partitionKeyOwnershipView = this.clusterPartitionBusinessLogic.addPartitionId(partitionId, getPartitionKeyOwnershipView());
        if(!partitionKeyOwnershipView.equals(updateOwnershipViewAtomically(partitionKeyOwnershipView, currentView)) ) {
            addPartitionId(partitionId);
        }
    }
}
