package com.fcusumano.push;

import com.fcusumano.push.TestPayloads.TestChannels;
import com.fcusumano.push.annotation.EnablePushMessaging;
import com.fcusumano.push.controller.StompController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.AbstractMessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.ImmutableMessageChannelInterceptor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Controller;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ClassUtils;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

//Attivazione di STOMP: classpath senza messaging, verifiche all'avvio e bean condivisi con SSE
@ExtendWith(OutputCaptureExtension.class)
class StompActivationTest {

    private static final String[] MESSAGING_PACKAGES = {"org.springframework.messaging", "org.springframework.web.socket"};
    private static final List<String> MESSAGING_JARS = List.of("spring-messaging-", "spring-websocket-");
    private static final String ASYNC_FALLBACK_WARNING = "Spring Boot did not create 'applicationTaskExecutor'";
    private static final List<String> CLIENT_CHANNELS = List.of("clientInboundChannel", "clientOutboundChannel");

    @Test
    void sseOnlyStartsWithoutMessagingOnClasspath() throws Exception {
        //Classpath dei test senza i jar del messaging, in un class loader che non vede quello dei test:
        //una classe SSE che riferisse il messaging fallirebbe qui con NoClassDefFoundError
        final Thread thread = Thread.currentThread();
        final ClassLoader testClassLoader = thread.getContextClassLoader();
        try (URLClassLoader classLoader = new URLClassLoader(classpathWithoutMessaging(), ClassLoader.getPlatformClassLoader())) {
            thread.setContextClassLoader(classLoader); //Spring legge annotazioni ed enum anche dal class loader del thread
            Assertions.assertFalse(ClassUtils.isPresent(Message.class.getName(), classLoader));
            final Class<?> contextType = classLoader.loadClass(AnnotationConfigApplicationContext.class.getName());
            final Class<?> configuration = classLoader.loadClass(SseOnlyConfiguration.class.getName());
            try (AutoCloseable context = (AutoCloseable) contextType.getConstructor(Class[].class).newInstance((Object) new Class<?>[] {configuration})) {
                final Method containsBean = contextType.getMethod("containsBean", String.class);
                Assertions.assertEquals(true, containsBean.invoke(context, "sseService"));
                Assertions.assertEquals(false, containsBean.invoke(context, "pushStompService"));
            }
        } finally {
            thread.setContextClassLoader(testClassLoader);
        }
    }

    @Test
    void stompWithoutMessagingFailsClearly() {
        new ApplicationContextRunner()
                .withClassLoader(new FilteredClassLoader(MESSAGING_PACKAGES))
                .withUserConfiguration(StompConfiguration.class)
                .run(context -> {
                    Assertions.assertNotNull(context.getStartupFailure());
                    final String rootCause = rootCauseMessage(context.getStartupFailure());
                    Assertions.assertTrue(rootCause.contains("spring-boot-starter-websocket"), rootCause);
                });
    }

    @Test
    void missingStompControllerFailsAtStartup() {
        final String failure = startupFailure(NoControllerApplication.class);
        Assertions.assertTrue(failure.contains("requires exactly one StompController bean, found 0"), failure);
    }

    @Test
    void twoStompControllersFailAtStartup() {
        final String failure = startupFailure(TwoControllersApplication.class);
        Assertions.assertTrue(failure.contains("requires exactly one StompController bean, found 2"), failure);
    }

    @Test
    void overlappingDestinationPrefixFailsAtStartup() {
        final String failure = startupFailure(OverlappingBrokerApplication.class);
        Assertions.assertTrue(failure.contains("Destination prefixes [/push] overlap the push destination prefix '/push'"), failure);
    }

    @Test
    void invalidDestinationPrefixFailsAtStartup() {
        final String failure = startupFailure(StompApplication.class, "push-manager.stomp.destination-prefix=push/");
        Assertions.assertTrue(failure.contains("push-manager.stomp.destination-prefix must start with '/' and must not end with '/': 'push/'"), failure);
    }

    @Test
    void sseAndStompServicesShareTheLibraryScheduler() {
        try (ConfigurableApplicationContext context = application(StompApplication.class).run()) {
            final Object taskScheduler = context.getBean("taskScheduler");
            Assertions.assertSame(taskScheduler, ReflectionTestUtils.getField(context.getBean("sseService"), "taskScheduler"));
            Assertions.assertSame(taskScheduler, ReflectionTestUtils.getField(context.getBean("pushStompService"), "taskScheduler"));
            //Lo scheduler del broker è un secondo candidato: senza qualifier l'iniezione fallirebbe
            Assertions.assertTrue(context.containsBean("messageBrokerTaskScheduler"));
        }
    }

    @Test
    void clientChannelsDoNotRunOnTheLibraryScheduler() {
        try (ConfigurableApplicationContext context = application(StompApplication.class).run()) {
            final ThreadPoolTaskScheduler taskScheduler = context.getBean("taskScheduler", ThreadPoolTaskScheduler.class);
            for (String channel : CLIENT_CHANNELS) {
                //Spring Boot sceglierebbe lo scheduler degli heartbeat SSE, unico AsyncTaskExecutor: il canale ha un pool dedicato
                final Executor executor = channelExecutor(context, channel);
                Assertions.assertNotSame(taskScheduler, executor, channel);
                Assertions.assertEquals(channel + "-", ((ThreadPoolTaskExecutor) executor).getThreadNamePrefix());
            }
            //Fase del ciclo di vita invariata: Spring la imposta solo sugli executor dei canali
            Assertions.assertEquals(new ThreadPoolTaskScheduler().getPhase(), taskScheduler.getPhase());
        }
    }

    @Test
    void applicationChannelExecutorIsKept() {
        try (ConfigurableApplicationContext context = application(ExecutorApplication.class).run()) {
            Assertions.assertEquals("app-inbound-", ((ThreadPoolTaskExecutor) channelExecutor(context, "clientInboundChannel")).getThreadNamePrefix());
            Assertions.assertNotSame(context.getBean("taskScheduler"), channelExecutor(context, "clientOutboundChannel"));
        }
    }

    @Test
    void applicationInterceptorsSeePushFramesFirst() {
        //Configurer dell'applicazione registrato dopo quello della libreria, con lo stesso ordine
        try (ConfigurableApplicationContext context = application(InterceptorApplication.class).run()) {
            final AbstractMessageChannel channel = context.getBean("clientInboundChannel", AbstractMessageChannel.class);
            final List<ChannelInterceptor> interceptors = channel.getInterceptors();
            final RecordingInterceptor recorder = context.getBean(RecordingInterceptor.class);
            final int application = interceptors.indexOf(recorder);
            Assertions.assertTrue(application >= 0 && application < interceptors.indexOf(context.getBean(StompController.class)), interceptors.toString());
            Assertions.assertInstanceOf(ImmutableMessageChannelInterceptor.class, interceptors.getLast());
            //La SUBSCRIBE della libreria, trattenuta dal controller, passa prima dall'interceptor dell'applicazione
            Assertions.assertFalse(channel.send(pushSubscribe()));
            Assertions.assertEquals(List.of("/push/USERS"), recorder.destinations);
        }
    }

    @Test
    void stompControllerMayUseMessagingTemplate() {
        //Il controller non viene più risolto durante la creazione del clientInboundChannel: nessun ciclo tra i bean
        try (ConfigurableApplicationContext context = application(TemplateApplication.class).run()) {
            Assertions.assertNotNull(context.getBean(TemplateStompEndpoint.class).template);
        }
    }

    @Test
    void asyncExecutorFallbackIsReported(CapturedOutput output) {
        try (ConfigurableApplicationContext _ = application(StompApplication.class).run()) {
            Assertions.assertTrue(output.getAll().contains(ASYNC_FALLBACK_WARNING));
        }
        final int start = output.getAll().length();
        try (ConfigurableApplicationContext context = application(StompApplication.class, "spring.task.execution.mode=force").run()) {
            Assertions.assertTrue(context.containsBean("applicationTaskExecutor"));
            Assertions.assertFalse(output.getAll().substring(start).contains(ASYNC_FALLBACK_WARNING));
            //Effetto collaterale documentato: Spring Boot esegue anche i canali dei client sull'executor di @Async
            for (String channel : CLIENT_CHANNELS) {
                Assertions.assertSame(context.getBean("applicationTaskExecutor"), channelExecutor(context, channel), channel);
            }
        }
    }

    private static SpringApplicationBuilder application(Class<?> application, String... properties) {
        return new SpringApplicationBuilder(application)
                .properties("server.port=0", "spring.main.banner-mode=off", "push-manager.heartbeat-enabled=false")
                .properties(properties);
    }

    //Messaggio della causa originaria del fallimento all'avvio
    private static String startupFailure(Class<?> application, String... properties) {
        try (ConfigurableApplicationContext _ = application(application, properties).run()) {
            return Assertions.fail("Application '" + application.getSimpleName() + "' should not start");
        } catch (RuntimeException e) {
            return rootCauseMessage(e);
        }
    }

    private static String rootCauseMessage(Throwable failure) {
        return NestedExceptionUtils.getMostSpecificCause(failure).getMessage();
    }

    private static Executor channelExecutor(ConfigurableApplicationContext context, String channel) {
        return context.getBean(channel, ExecutorSubscribableChannel.class).getExecutor();
    }

    //Voci del classpath dei test tranne i jar di spring-messaging e spring-websocket
    private static URL[] classpathWithoutMessaging() throws MalformedURLException {
        final List<URL> urls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            final Path path = Path.of(entry);
            if (MESSAGING_JARS.stream().noneMatch(path.getFileName().toString()::startsWith)) urls.add(path.toUri().toURL());
        }
        return urls.toArray(URL[]::new);
    }

    //SUBSCRIBE della libreria come la decodifica StompSubProtocolHandler
    private static Message<byte[]> pushSubscribe() {
        final StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        accessor.setSessionId("SESSION-1");
        accessor.setSubscriptionId("sub-0");
        accessor.setDestination("/push/USERS");
        accessor.setLeaveMutable(true);
        return MessageBuilder.createMessage(new byte[0], accessor.getMessageHeaders());
    }

    @Configuration(proxyBeanMethods = false)
    @EnablePushMessaging(stomp = true)
    static class StompConfiguration {}

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class StompApplication {

        @Bean
        StompFlowTest.TestStompEndpoint testStompEndpoint() {
            return new StompFlowTest.TestStompEndpoint();
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class NoControllerApplication {}

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class TwoControllersApplication {

        @Bean
        StompFlowTest.TestStompEndpoint testStompEndpoint() {
            return new StompFlowTest.TestStompEndpoint();
        }

        @Bean
        OtherStompEndpoint otherStompEndpoint() {
            return new OtherStompEndpoint();
        }
    }

    //Broker dell'applicazione dentro il prefisso riservato alla libreria
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class OverlappingBrokerApplication {

        @Bean
        StompFlowTest.TestStompEndpoint testStompEndpoint() {
            return new StompFlowTest.TestStompEndpoint();
        }

        @Bean
        WebSocketMessageBrokerConfigurer pushBrokerConfigurer() {
            return new WebSocketMessageBrokerConfigurer() {
                @Override
                public void configureMessageBroker(MessageBrokerRegistry registry) {
                    registry.enableSimpleBroker("/push");
                }
            };
        }
    }

    //Executor del canale in ingresso scelto dall'applicazione
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class ExecutorApplication {

        @Bean
        StompFlowTest.TestStompEndpoint testStompEndpoint() {
            return new StompFlowTest.TestStompEndpoint();
        }

        @Bean
        WebSocketMessageBrokerConfigurer inboundExecutorConfigurer() {
            return new WebSocketMessageBrokerConfigurer() {
                @Override
                public void configureClientInboundChannel(ChannelRegistration registration) {
                    final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
                    executor.setThreadNamePrefix("app-inbound-");
                    registration.executor(executor);
                }
            };
        }
    }

    //Interceptor dell'applicazione da un @Bean della classe di avvio, non ordinato
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class InterceptorApplication {

        @Bean
        StompFlowTest.TestStompEndpoint testStompEndpoint() {
            return new StompFlowTest.TestStompEndpoint();
        }

        @Bean
        RecordingInterceptor recordingInterceptor() {
            return new RecordingInterceptor();
        }

        @Bean
        WebSocketMessageBrokerConfigurer interceptorConfigurer(RecordingInterceptor recordingInterceptor) {
            return new WebSocketMessageBrokerConfigurer() {
                @Override
                public void configureClientInboundChannel(ChannelRegistration registration) {
                    registration.interceptors(recordingInterceptor);
                }
            };
        }
    }

    //Controller che usa il template di Spring, come un'applicazione con metodi @MessageMapping
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnablePushMessaging(stomp = true)
    static class TemplateApplication {

        @Bean
        TemplateStompEndpoint templateStompEndpoint(SimpMessagingTemplate template) {
            return new TemplateStompEndpoint(template);
        }
    }

    @Controller
    static class OtherStompEndpoint extends StompController<TestChannels> {}

    @Controller
    static class TemplateStompEndpoint extends StompController<TestChannels> {

        private final SimpMessagingTemplate template;

        TemplateStompEndpoint(SimpMessagingTemplate template) {
            this.template = template;
        }

        @MessageMapping("/ping")
        void ping() {
            template.convertAndSend("/topic/pong", "PONG-1");
        }
    }

    //Destinazioni dei frame visti, come un interceptor di autorizzazione dell'applicazione
    static final class RecordingInterceptor implements ChannelInterceptor {

        private final List<String> destinations = new CopyOnWriteArrayList<>();

        @Override
        public Message<?> preSend(Message<?> message, MessageChannel channel) {
            final String destination = SimpMessageHeaderAccessor.getDestination(message.getHeaders());
            if (destination != null) destinations.add(destination);
            return message;
        }
    }
}
