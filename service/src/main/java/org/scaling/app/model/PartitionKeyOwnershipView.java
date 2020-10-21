package org.scaling.app.model;

import lombok.Getter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


@Getter
public class PartitionKeyOwnershipView {
    private final Map<String, String> partitionKeyToIdMap;
    private final Map<String, PartitionKeyOwnerState> partitionIdToKeyLoadMap;
    private final SortedSet<PartitionKeyOwnerState> partitionKeyOwnerStates;

    public PartitionKeyOwnershipView(SortedSet<PartitionKeyOwnerState>  partitionKeyOwnerStates) {
        SortedSet<PartitionKeyOwnerState> viewClone = Collections.unmodifiableSortedSet(partitionKeyOwnerStates);
        Map<String, String> updatedPartitionKeyToIdMap = new ConcurrentHashMap<>();
        viewClone
                .stream()
                .forEach( state -> state.getPartitionKeys().forEach(key -> updatedPartitionKeyToIdMap.put(key, state.getPartitionId())));
        Map<String, PartitionKeyOwnerState> updatedPartitionIdToKeyMap = viewClone.stream().collect(Collectors.toMap(PartitionKeyOwnerState::getPartitionId, Function.identity()));
        this.partitionKeyOwnerStates = viewClone;
        this.partitionKeyToIdMap = Collections.unmodifiableMap(updatedPartitionKeyToIdMap);
        this.partitionIdToKeyLoadMap = Collections.unmodifiableMap(updatedPartitionIdToKeyMap);
    }

    public PartitionKeyOwnershipView getNewView(String partitionKey, String partitionId) {
        return getNewView(Collections.singletonMap(partitionKey, partitionId), true);
    }

    public PartitionKeyOwnershipView getNewView(Map<String, String> proposedKeyToIdMapping) {
        return getNewView(proposedKeyToIdMapping, false);
    }

    private PartitionKeyOwnershipView getNewView(Map<String, String> proposedKeyToIdMapping, boolean update) {
        Map<String, PartitionKeyOwnerState> unChangedPartitionIdMap = update ? partitionIdToKeyLoadMap : Collections.emptyMap();
        Map<String, PartitionKeyOwnerState> updatedPartitionIdToKeyLoadMap = new HashMap<>(unChangedPartitionIdMap);
        proposedKeyToIdMapping.entrySet().forEach( proposedKeyToIdMappingEntry -> {
            String partitionId = proposedKeyToIdMappingEntry.getValue();
            PartitionKeyOwnerState partitionKeysForId = updatedPartitionIdToKeyLoadMap.get(partitionId);
            if (partitionKeysForId == null) {
                partitionKeysForId = new PartitionKeyOwnerState(partitionId, Collections.emptySortedSet());
            }
            TreeSet<String> newPartitionKeys = new TreeSet<>(partitionKeysForId.getPartitionKeys());
            newPartitionKeys.add(proposedKeyToIdMappingEntry.getKey());
            PartitionKeyOwnerState partitionKeyOwnerState = new PartitionKeyOwnerState(partitionId, newPartitionKeys);
            updatedPartitionIdToKeyLoadMap.put(partitionId, partitionKeyOwnerState);
        });
        return new PartitionKeyOwnershipView(new TreeSet<>(updatedPartitionIdToKeyLoadMap.values()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionKeyOwnershipView that = (PartitionKeyOwnershipView) o;
        return Objects.equals(partitionKeyOwnerStates, that.partitionKeyOwnerStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKeyOwnerStates);
    }
}
