package org.scaling.app.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
@ToString
public class Event implements Serializable {
    private String id;
    private String value;
    private String partitionIdField;

    public EventKey getEventKey() {
        return new EventKey(this.id);
    }
}
