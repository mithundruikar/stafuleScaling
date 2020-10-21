package org.geode.scaling.stafuleScaling;

import org.entry.ExternalIncomigEventListener;
import org.scaling.app.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EventGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventGenerator.class);

    private ExternalIncomigEventListener externalIncomigEventListener;
    private long counter = 0;
    private long MAX_BUFFER_SIZE = 1000;
    private Set<String> nodeIds;

    public EventGenerator(ExternalIncomigEventListener externalIncomigEventListener, Set<String> nodeIds) {
        this.nodeIds = nodeIds;
        this.externalIncomigEventListener = externalIncomigEventListener;
    }

    @PostConstruct
    public void init() {
        LOGGER.info("Starting event generator thread");
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(this::load, 20, 1, TimeUnit.SECONDS);
    }


    public void load() {
        try {
            nodeIds.forEach( nodeId -> {
                Event event = new Event(String.valueOf(counter++ % MAX_BUFFER_SIZE), String.valueOf(counter), nodeId);
                this.externalIncomigEventListener.handleEvent(event);
            });
        } catch (RuntimeException re) {
            re.printStackTrace();
        }
    }
}
