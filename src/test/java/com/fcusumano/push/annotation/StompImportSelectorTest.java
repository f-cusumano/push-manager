package com.fcusumano.push.annotation;

import com.fcusumano.push.StompPushAutoConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.core.type.AnnotationMetadata;

import java.io.IOException;

class StompImportSelectorTest {

    @Test
    void stompAutoConfigNameIsCurrent() {
        //Il selector nomina la configurazione come stringa: un rename deve rompere questo test
        Assertions.assertEquals(StompPushAutoConfig.class.getName(), StompImportSelector.STOMP_AUTO_CONFIG);
    }

    @Test
    void stompFalseImportsNothing() {
        Assertions.assertArrayEquals(new String[0], selector(getClass().getClassLoader()).selectImports(AnnotationMetadata.introspect(SseOnly.class)));
    }

    @Test
    void stompTrueImportsStompAutoConfig() {
        Assertions.assertArrayEquals(new String[] {StompImportSelector.STOMP_AUTO_CONFIG},
                selector(getClass().getClassLoader()).selectImports(AnnotationMetadata.introspect(SseAndStomp.class)));
    }

    @Test
    void stompWithoutMessagingFails() throws IOException {
        try (FilteredClassLoader classLoader = new FilteredClassLoader("org.springframework.messaging", "org.springframework.web.socket")) {
            //SSE resta attivabile senza messaging
            Assertions.assertArrayEquals(new String[0], selector(classLoader).selectImports(AnnotationMetadata.introspect(SseOnly.class)));
            try {
                selector(classLoader).selectImports(AnnotationMetadata.introspect(SseAndStomp.class));
                Assertions.fail("STOMP without spring-messaging should fail");
            } catch (IllegalStateException e) {
                Assertions.assertTrue(e.getMessage().contains("add spring-boot-starter-websocket"), e.getMessage());
            }
        }
    }

    private static StompImportSelector selector(ClassLoader classLoader) {
        final StompImportSelector selector = new StompImportSelector();
        selector.setBeanClassLoader(classLoader);
        return selector;
    }

    @EnablePushMessaging
    static class SseOnly {}

    @EnablePushMessaging(stomp = true)
    static class SseAndStomp {}
}
