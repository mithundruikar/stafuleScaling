package org.entry;

import org.scaling.app.model.Event;

public interface EventDispatcher {
    boolean handleEvent(Event event);
}
