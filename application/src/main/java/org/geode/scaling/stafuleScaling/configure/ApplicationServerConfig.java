package org.geode.scaling.stafuleScaling.configure;

import org.entry.EventDispatcher;
import org.entry.ExternalIncomigEventListener;
import org.geode.scaling.config.GeodeCacheConfig;
import org.geode.scaling.stafuleScaling.EventGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Arrays;
import java.util.HashSet;

@Configuration
@Import({ GeodeCacheConfig.class})
public class ApplicationServerConfig {
    @Bean
    public ExternalIncomigEventListener externalIncomigEventListener(EventDispatcher eventDispatcher) {
        return new ExternalIncomigEventListener(eventDispatcher);
    }

    @Bean
    public EventGenerator eventGenerator(ExternalIncomigEventListener externalIncomigEventListener) {
        return new EventGenerator(externalIncomigEventListener, new HashSet<>(Arrays.asList("node1", "node2", "node3")));
    }
}
