package org.entry;

import org.scaling.app.model.Event;

public class ExternalIncomigEventListener {
    private EventDispatcher eventDispatcher;
    public ExternalIncomigEventListener(EventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    public void handleEvent(Event event) {
        this.eventDispatcher.handleEvent(event);
    }
}
