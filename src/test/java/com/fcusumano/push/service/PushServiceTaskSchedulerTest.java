package com.fcusumano.push.service;

import com.fcusumano.push.annotation.EnablePushMessaging;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

//Scheduler degli heartbeat SSE: come senza qualifier quello unico o @Primary; se è ambiguo quello della libreria
class PushServiceTaskSchedulerTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(SseApplication.class);

    @Test
    void librarySchedulerWhenAlone() {
        runner.run(context -> Assertions.assertSame(context.getBean("taskScheduler"), sseScheduler(context)));
    }

    @Test
    void primaryApplicationSchedulerIsKept() {
        runner.withUserConfiguration(PrimarySchedulerConfiguration.class)
                .run(context -> Assertions.assertSame(context.getBean("appScheduler"), sseScheduler(context)));
    }

    @Test
    void librarySchedulerWhenAmbiguous() {
        //Due scheduler senza @Primary: senza la scelta della libreria l'avvio fallirebbe
        runner.withUserConfiguration(SchedulerConfiguration.class)
                .run(context -> Assertions.assertSame(context.getBean("taskScheduler"), sseScheduler(context)));
    }

    private static TaskScheduler sseScheduler(ApplicationContext context) {
        return context.getBean(SseService.class).taskScheduler;
    }

    private static ThreadPoolTaskScheduler scheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("app-scheduler-");
        return scheduler;
    }

    @Configuration(proxyBeanMethods = false)
    @EnablePushMessaging
    static class SseApplication {}

    @Configuration(proxyBeanMethods = false)
    static class PrimarySchedulerConfiguration {

        @Bean
        @Primary
        TaskScheduler appScheduler() {
            return scheduler();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SchedulerConfiguration {

        @Bean
        TaskScheduler appScheduler() {
            return scheduler();
        }
    }
}
