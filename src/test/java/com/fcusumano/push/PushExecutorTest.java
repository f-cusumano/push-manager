package com.fcusumano.push;

import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.SseEventManager;
import com.fcusumano.push.event.SsePushEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;

import static com.fcusumano.push.TestPayloads.user1;

//Consegna degli eventi sull'executor della libreria, senza togliere a Spring Boot il suo
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(classes = SseFlowTest.TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"push-manager.heartbeat-enabled=false", "logging.level.com.fcusumano.push.event=DEBUG"})
class PushExecutorTest {

    @Autowired
    private ApplicationContext context;
    @Autowired
    private SseEventManager sseEventManager;
    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    void applicationTaskExecutorIsKept() {
        //pushExecutor non è candidato all'autowiring: Spring Boot crea comunque il proprio executor
        Assertions.assertTrue(context.containsBean(PushAutoConfig.PUSH_EXECUTOR));
        Assertions.assertTrue(context.containsBean("applicationTaskExecutor"));
    }

    @Test
    void eventsAreHandledOnPushExecutor(CapturedOutput output) throws InterruptedException {
        transactionTemplate.executeWithoutResult(_ ->
                sseEventManager.publishEvent(SsePushEvent.of(user1(), EventType.CREATED, TestChannels.USERS)));
        final long deadline = System.currentTimeMillis() + 5000;
        while (!output.getAll().contains("Captured event") && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        final String captured = output.getAll().lines().filter(l -> l.contains("Captured event")).findFirst().orElse("");
        Assertions.assertTrue(captured.contains("push-executor-"), captured);
    }
}
