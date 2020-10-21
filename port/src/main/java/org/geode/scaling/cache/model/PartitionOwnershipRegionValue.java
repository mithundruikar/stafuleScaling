package org.geode.scaling.cache.model;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.scaling.app.model.PartitionKeyOwnerState;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

@Value
@EqualsAndHashCode
public class PartitionOwnershipRegionValue implements Serializable {

    private String globalStateKey;
    private SortedSet<PartitionKeyOwnerState>  partitionKeyOwnerStateList;
}
