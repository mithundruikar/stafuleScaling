package org.geode.scaling.app.cluster;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.scaling.app.model.Event;
import org.scaling.app.service.EventConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GeodeNodeEventsListener implements AsyncEventListener {
    private String serverName;
    private Cache cache;
    private EventConsumer eventConsumer;
    private final List<Operation> permittedOperations = Arrays.asList(Operation.PUT_IF_ABSENT, Operation.PUTALL_CREATE, Operation.PUTALL_UPDATE, Operation.CREATE, Operation.UPDATE);

    public GeodeNodeEventsListener(String serverName, Cache cache, EventConsumer eventConsumer) {
        this.serverName = serverName;
        this.cache = cache;
        this.eventConsumer = eventConsumer;
    }

    @Override
    public boolean processEvents(List<AsyncEvent> list) {
        list.stream()
                //.filter(event -> permittedOperations.contains(event.getOperation()))
                .map(event -> event.getDeserializedValue())
                .map(event -> (Event) event)
                .forEach(this.eventConsumer::handleEvents);
        return true;
    }
}
