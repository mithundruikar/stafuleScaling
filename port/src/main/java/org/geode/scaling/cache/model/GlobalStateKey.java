package org.geode.scaling.cache.model;

import lombok.EqualsAndHashCode;
import lombok.Value;

import java.io.Serializable;

@Value
@EqualsAndHashCode
public class GlobalStateKey implements Serializable {
    public static final String GLOBAL_PARTITION_OWNERSHIP_MAP_KEY = "GLOBAL_PARTITION_MAP";
    private String globalStateKey;

}
