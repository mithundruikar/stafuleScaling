package org.scaling.app.cluster;

import org.scaling.app.model.PartitionKeyOwnerState;
import org.scaling.app.model.PartitionKeyOwnershipView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class ClusterPartitionBusinessLogic {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterPartitionBusinessLogic.class);

    public PartitionKeyOwnershipView removePartitionId(String partitionId, PartitionKeyOwnershipView partitionKeyOwnershipView) {
        PartitionKeyOwnerState partitionKeyOwnerState = partitionKeyOwnershipView.getPartitionIdToKeyLoadMap().get(partitionId);
        Set<String> otherIds = partitionKeyOwnershipView.getPartitionIdToKeyLoadMap().keySet().stream().filter(pId -> !pId.equals(partitionId)).collect(Collectors.toSet());
        SortedSet<String> partitionKeys = partitionKeyOwnerState.getPartitionKeys();

        Iterator<String> iterator = partitionKeys.iterator();
        Map<String, String>  partitionKeyOwnerStates = new HashMap<>();
        while(!partitionKeys.isEmpty()) {
            otherIds.forEach(pId -> {
                String partitionKey = iterator.next();
                iterator.remove();
                partitionKeyOwnerStates.put(partitionKey, pId);
            });
        }
        PartitionKeyOwnershipView newView = partitionKeyOwnershipView.getNewView(partitionKeyOwnerStates);
        LOGGER.info("Assigned on removal of partition id {} old view {} new view {}",
                partitionId,
                partitionKeyOwnershipView.getPartitionIdToKeyLoadMap(),
                newView.getPartitionIdToKeyLoadMap());
        return newView;
    }

    public PartitionKeyOwnershipView addPartitionId(String partitionId, PartitionKeyOwnershipView partitionKeyOwnershipView) {
        Map<String, String> partitionKeyToIdMap = new HashMap<>(partitionKeyOwnershipView.getPartitionKeyToIdMap());
        Set<String> otherIds = partitionKeyOwnershipView.getPartitionIdToKeyLoadMap().keySet().stream().filter(pId -> !pId.equals(partitionId)).collect(Collectors.toSet());
        int roughPartitionKeysToAssign = partitionKeyToIdMap.keySet().size() / (otherIds.size() + 1);
        if(roughPartitionKeysToAssign < 1 ){
            return partitionKeyOwnershipView;
        }
        Iterator<Map.Entry<String, String>> iterator = partitionKeyToIdMap.entrySet().iterator();
        IntStream.range(0, roughPartitionKeysToAssign).forEach( i -> {
            Map.Entry<String, String> next = iterator.next();
            iterator.remove();
            partitionKeyToIdMap.put(next.getKey(), partitionId);
        });
        PartitionKeyOwnershipView newView = partitionKeyOwnershipView.getNewView(partitionKeyToIdMap);
        LOGGER.info("Assigned on addition of partition id {} old view {} new view {}",
                partitionId,
                partitionKeyOwnershipView.getPartitionIdToKeyLoadMap(),
                newView.getPartitionIdToKeyLoadMap());
        return newView;
    }

    public PartitionKeyOwnershipView assignPartition(String partitionKey, PartitionKeyOwnershipView partitionKeyOwnershipView) {
        Optional<Map.Entry<String, PartitionKeyOwnerState>> min = partitionKeyOwnershipView
                .getPartitionIdToKeyLoadMap()
                .entrySet()
                .stream()
                .min(Comparator.comparing(entry -> entry.getValue().getPartitionKeys().size()));
        if(!min.isPresent()) {
            throw new IllegalStateException("not partitions present in the view");
        }
        String assignedPartitionId = min.get().getKey();
        PartitionKeyOwnershipView newView = partitionKeyOwnershipView.getNewView(partitionKey, assignedPartitionId);
        LOGGER.info("Assigned partition id {} for key {} based on known id to key view {} new view {}",
                assignedPartitionId,
                partitionKey,
                partitionKeyOwnershipView.getPartitionIdToKeyLoadMap(),
                newView.getPartitionIdToKeyLoadMap());
        return newView;
    }
}
