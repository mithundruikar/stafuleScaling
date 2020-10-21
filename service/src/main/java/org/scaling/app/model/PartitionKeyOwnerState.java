package org.scaling.app.model;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.io.Serializable;
import java.util.SortedSet;


@Value
@AllArgsConstructor
public class PartitionKeyOwnerState implements Serializable, Comparable<PartitionKeyOwnerState> {
    private String partitionId;
    private SortedSet<String> partitionKeys;


    @Override
    public int compareTo(PartitionKeyOwnerState o) {
        return this.partitionId.compareTo(o.getPartitionId());
    }
}
