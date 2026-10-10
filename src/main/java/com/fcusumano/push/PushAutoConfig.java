package com.fcusumano.push;

import com.fcusumano.push.event.SseEventManager;
import com.fcusumano.push.properties.PushProperties;
import com.fcusumano.push.registry.SseRegistry;
import com.fcusumano.push.service.SseService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@EnableAsync
@AutoConfiguration
@EnableConfigurationProperties(PushProperties.class)
public class PushAutoConfig {

    @Bean("sseRegistry")
    public SseRegistry sseRegistry() {
        return new SseRegistry();
    }

    @Bean("sseService")
    public SseService sseService() {
        return new SseService();
    }

    @Bean("sseEventManager")
    public SseEventManager sseEventManager(SseRegistry registry) {
        return new SseEventManager(registry);
    }

    @Bean("taskScheduler")
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("push-scheduler-");
        scheduler.initialize();
        return scheduler;
    }
}
