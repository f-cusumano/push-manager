package com.fcusumano.push;

import com.fcusumano.push.controller.StompController;
import com.fcusumano.push.properties.StompProperties;
import com.fcusumano.push.util.StompDestinations;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.core.Ordered;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.annotation.support.SimpAnnotationMethodMessageHandler;
import org.springframework.messaging.simp.broker.AbstractBrokerMessageHandler;
import org.springframework.messaging.simp.broker.SimpleBrokerMessageHandler;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.support.AbstractMessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ImmutableMessageChannelInterceptor;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.StompWebSocketEndpointRegistration;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Configura l'infrastruttura STOMP di Spring per push-manager: registra l'endpoint della libreria e, a contesto completo,
 * aggiunge lo {@link StompController} dell'applicazione al {@code clientInboundChannel} dopo gli interceptor configurati,
 * qualunque sia l'ordine dei configurer, poi verifica la configurazione.
 * <p>
 * Non tocca broker, prefissi, convertitori e trasporto: restano dell'applicazione
 * ({@code configureMessageBroker} vale solo per l'ultimo configurer che lo chiama). Degli executor dei canali
 * dei client corregge solo la scelta di Spring Boot quando ricade sullo scheduler della libreria.
 * </p>
 * <p>
 * Se l'applicazione non configura né broker né prefissi, il broker di default di Spring accetterebbe ogni destinazione:
 * le {@code SUBSCRIBE} e le {@code SEND} fuori dal prefisso della libreria vengono allora trattenute.
 * </p>
 *
 * @author Francesco Cusumano
 */
final class StompPushConfigurer implements WebSocketMessageBrokerConfigurer, SmartInitializingSingleton, Ordered {

    private static final Logger log = LoggerFactory.getLogger(StompPushConfigurer.class);
    private static final String LIBRARY_SCHEDULER = "taskScheduler"; //Bean di PushAutoConfig: heartbeat SSE
    private static final String CLIENT_INBOUND_CHANNEL = "clientInboundChannel";

    private final StompProperties properties;
    private final StompDestinations destinations;
    private final ObjectProvider<StompController<?>> controllers;
    private final ListableBeanFactory beanFactory;


    StompPushConfigurer(StompProperties properties, ObjectProvider<StompController<?>> controllers, ListableBeanFactory beanFactory) {
        this.properties = properties;
        this.destinations = StompDestinations.of(properties.destinationPrefix());
        this.controllers = controllers;
        this.beanFactory = beanFactory;
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        final String endpoint = properties.endpoint();
        if (!StringUtils.hasText(endpoint)) {
            log.info("No STOMP endpoint registered by push-manager: clients must use the application's endpoints");
            return;
        }
        final StompWebSocketEndpointRegistration registration = registry.addEndpoint(endpoint);
        final List<String> allowedOriginPatterns = properties.allowedOriginPatterns();
        if (!CollectionUtils.isEmpty(allowedOriginPatterns)) registration.setAllowedOriginPatterns(allowedOriginPatterns.toArray(String[]::new));
        log.info("STOMP endpoint '{}' registered by push-manager", endpoint);
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        keepOffLibraryScheduler(registration, WebSocketMessageBrokerConfigurer::configureClientInboundChannel, "clientInboundChannel-");
    }

    @Override
    public void configureClientOutboundChannel(ChannelRegistration registration) {
        keepOffLibraryScheduler(registration, WebSocketMessageBrokerConfigurer::configureClientOutboundChannel, "clientOutboundChannel-");
    }

    @Override
    public int getOrder() { //Subito dopo il configurer di Spring Boot (ordine 0): un executor scelto dall'applicazione resta il suo
        return 1;
    }

    //Verifiche e interceptor a contesto completo: nessun frame arriva prima dell'avvio del server
    @Override
    public void afterSingletonsInstantiated() {
        final StompController<?> controller = singleController();
        final List<AbstractBrokerMessageHandler> brokers = beanFactory.getBeanProvider(AbstractBrokerMessageHandler.class).stream().toList();
        final List<SimpAnnotationMethodMessageHandler> annotationHandlers =
                beanFactory.getBeanProvider(SimpAnnotationMethodMessageHandler.class).stream().toList();
        checkDestinationPrefixes(brokers, annotationHandlers);
        final boolean defaultBroker = isDefaultBroker(brokers, annotationHandlers);
        addInterceptors(controller, defaultBroker);
        if (!beanFactory.containsBean("applicationTaskExecutor") && !beanFactory.containsBean("taskExecutor")
            && beanFactory.getBeanNamesForType(AsyncConfigurer.class, false, false).length == 0) {
            log.warn("Spring Boot did not create 'applicationTaskExecutor' because the WebSocket message broker registers its own executors: "
                     + "the application's own @Async methods fall back to a new thread per call (push delivery uses 'pushExecutor'). "
                     + "Set 'spring.task.execution.mode=force' to restore it "
                     + "(Spring Boot then runs the WebSocket client channels on it too).");
        }
        if (beanFactory.getBeanProvider(Validator.class).getIfUnique() == null) {
            log.warn("No unique Jakarta Validator: STOMP filters are not bean-validated (add spring-boot-starter-validation)");
        }
        if (defaultBroker) {
            log.info("No message broker configured by the application: SUBSCRIBE and SEND frames outside '{}' are discarded "
                     + "(configure it with a WebSocketMessageBrokerConfigurer to use it)", destinations.prefix());
        }
        log.info("STOMP push ready: endpoint '{}', SUBSCRIBE '{}/{channel}', SEND '{}/filter/{subscription-id}'",
                properties.endpoint(), destinations.prefix(), destinations.prefix());
    }

    private StompController<?> singleController() {
        final List<StompController<?>> found = controllers.stream().toList();
        if (found.size() != 1) {
            throw new IllegalStateException("@EnablePushMessaging(stomp = true) requires exactly one StompController bean, found " + found.size()
                                            + ": declare e.g. '@Controller class PushStompEndpoint extends StompController<Channels> {}'");
        }
        return found.getFirst();
    }

    //Prefissi dell'applicazione o del broker dentro quello della libreria: i frame dell'applicazione verrebbero trattenuti
    private void checkDestinationPrefixes(List<AbstractBrokerMessageHandler> brokers, List<SimpAnnotationMethodMessageHandler> annotationHandlers) {
        final List<String> conflicts = Stream.concat(
                        annotationHandlers.stream().map(SimpAnnotationMethodMessageHandler::getDestinationPrefixes),
                        brokers.stream().map(AbstractBrokerMessageHandler::getDestinationPrefixes))
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(this::insidePushPrefix)
                .distinct()
                .toList();
        if (!conflicts.isEmpty()) {
            throw new IllegalStateException("Destination prefixes " + conflicts + " overlap the push destination prefix '"
                                            + destinations.prefix() + "': change push-manager.stomp.destination-prefix");
        }
    }

    //Un prefisso che contiene il nostro (es. '/') è ammesso: i frame della libreria vengono trattenuti prima
    private boolean insidePushPrefix(String prefix) {
        return (prefix.endsWith("/") ? prefix : prefix + "/").startsWith(destinations.prefix() + "/");
    }

    //Nessun broker né prefisso configurato dall'applicazione: il broker semplice di default di Spring accetta ogni destinazione
    private static boolean isDefaultBroker(List<AbstractBrokerMessageHandler> brokers, List<SimpAnnotationMethodMessageHandler> annotationHandlers) {
        return !brokers.isEmpty()
               && brokers.stream().allMatch(broker -> broker instanceof SimpleBrokerMessageHandler && CollectionUtils.isEmpty(broker.getDestinationPrefixes()))
               && annotationHandlers.stream().allMatch(handler -> CollectionUtils.isEmpty(handler.getDestinationPrefixes()));
    }

    //Davanti all'ImmutableMessageChannelInterceptor che Spring accoda per ultimo: dopo gli interceptor di tutti i configurer
    private void addInterceptors(StompController<?> controller, boolean defaultBroker) {
        final AbstractMessageChannel channel = beanFactory.getBean(CLIENT_INBOUND_CHANNEL, AbstractMessageChannel.class);
        final List<ChannelInterceptor> interceptors = channel.getInterceptors();
        final int index = IntStream.range(0, interceptors.size())
                .filter(i -> interceptors.get(i) instanceof ImmutableMessageChannelInterceptor)
                .findFirst()
                .orElse(interceptors.size());
        channel.addInterceptor(index, controller);
        if (defaultBroker) channel.addInterceptor(index + 1, new DefaultBrokerGuard()); //Dopo il controller: vede solo i frame non della libreria
    }

    //Spring Boot dà ai canali dei client l'unico AsyncTaskExecutor che trova, spesso lo scheduler della libreria (2 thread, heartbeat SSE):
    //in quel caso il canale riceve un pool dedicato, come senza Spring Boot
    private void keepOffLibraryScheduler(ChannelRegistration registration, BiConsumer<WebSocketMessageBrokerConfigurer, ChannelRegistration> configure,
                                         String threadNamePrefix) {
        final ExecutorProbe probe = new ExecutorProbe();
        for (WebSocketMessageBrokerConfigurer configurer : precedingConfigurers()) {
            configure.accept(configurer, probe); //Ripetuti su una registrazione di prova: ChannelRegistration non espone l'executor scelto
        }
        if (probe.executor() != beanFactory.getBean(LIBRARY_SCHEDULER)) return;
        log.debug("Spring Boot chose '{}' for the WebSocket client channel: '{}' pool used instead", LIBRARY_SCHEDULER, threadNamePrefix);
        registration.executor(defaultChannelExecutor(threadNamePrefix));
    }

    //Configurer che Spring esegue prima di questo
    private List<WebSocketMessageBrokerConfigurer> precedingConfigurers() {
        return beanFactory.getBeanProvider(WebSocketMessageBrokerConfigurer.class).orderedStream()
                .takeWhile(configurer -> configurer != this)
                .toList();
    }

    //Come il pool di default di Spring per i canali dei client: 2 thread per core, rilasciati se inattivi
    private static ThreadPoolTaskExecutor defaultChannelExecutor(String threadNamePrefix) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(Runtime.getRuntime().availableProcessors() * 2);
        executor.setAllowCoreThreadTimeOut(true);
        executor.setThreadNamePrefix(threadNamePrefix);
        return executor;
    }

    //Registrazione di prova: ChannelRegistration espone l'executor configurato solo alle sottoclassi
    private static final class ExecutorProbe extends ChannelRegistration {

        private Executor executor() {
            return hasExecutor() ? getExecutor(SyncTaskExecutor::new, _ -> {}) : null;
        }
    }

    //Con il broker di default trattiene SUBSCRIBE e SEND che non sono della libreria: il broker farebbe da relay anonimo tra i client
    private static final class DefaultBrokerGuard implements ChannelInterceptor {

        @Override
        public Message<?> preSend(Message<?> message, MessageChannel channel) {
            final MessageHeaders headers = message.getHeaders();
            final SimpMessageType type = SimpMessageHeaderAccessor.getMessageType(headers);
            final String sessionId = SimpMessageHeaderAccessor.getSessionId(headers);
            if (sessionId == null || (type != SimpMessageType.SUBSCRIBE && type != SimpMessageType.MESSAGE)) return message;
            log.debug("{} to '{}' from session '{}' discarded: no message broker configured by the application",
                    type, SimpMessageHeaderAccessor.getDestination(headers), sessionId);
            return null;
        }
    }
}
