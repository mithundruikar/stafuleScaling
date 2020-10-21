package org.scaling.app.service;


import org.scaling.app.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentSkipListSet;

public class EventConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private ConcurrentSkipListSet<String> processedIds = new ConcurrentSkipListSet<>();

    public void handleEvents(Event event) {
        if(notDuplicate(event)) {
            LOGGER.info("processing event {}", event);
        }
    }

    public boolean notDuplicate(Event event) {
        return processedIds.add(event.getId());
    }

}
