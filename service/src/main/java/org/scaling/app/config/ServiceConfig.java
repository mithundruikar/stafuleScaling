package org.scaling.app.config;

import org.scaling.app.cluster.ClusterPartitionBusinessLogic;
import org.scaling.app.service.EventConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceConfig {


    @Bean
    public EventConsumer eventConsumer() {
        return new EventConsumer();
    }

    @Bean
    public ClusterPartitionBusinessLogic clusterPartitionBusinessLogic() {
        return new ClusterPartitionBusinessLogic();
    }

}
