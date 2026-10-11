package com.fcusumano.push;

import com.fcusumano.push.controller.StompController;
import com.fcusumano.push.event.StompEventManager;
import com.fcusumano.push.properties.StompProperties;
import com.fcusumano.push.registry.StompRegistry;
import com.fcusumano.push.service.StompService;
import com.fcusumano.push.util.StompDestinations;
import jakarta.validation.Validator;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.function.SingletonSupplier;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import tools.jackson.databind.json.JsonMapper;

import java.util.function.Supplier;


/**
 * Auto-configurazione STOMP di push-manager, importata solo con {@code @EnablePushMessaging(stomp = true)}.
 * <p>Registra, in aggiunta ai bean SSE:</p>
 * <ul>
 *     <li>{@link StompRegistry}, il registro dei client STOMP per canale e per sessione;</li>
 *     <li>{@link StompService}, la logica di sottoscrizione, filtro, disiscrizione e chiusura delle sessioni;</li>
 *     <li>{@link StompEventManager}, da iniettare nell'applicazione per pubblicare eventi;</li>
 *     <li>il configurer che registra l'endpoint della libreria e lo {@link StompController} dell'applicazione
 *         come interceptor del {@code clientInboundChannel};</li>
 *     <li>le {@link StompProperties} con prefisso {@code push-manager.stomp}.</li>
 * </ul>
 * <p>
 * Abilita l'infrastruttura di Spring con {@link EnableWebSocketMessageBroker}, condivisa con l'eventuale
 * configurazione WebSocket dell'applicazione.
 * </p>
 *
 * @author Francesco Cusumano
 */
@AutoConfiguration
@EnableWebSocketMessageBroker //Deduplicata da Spring se anche l'applicazione la dichiara
@EnableConfigurationProperties(StompProperties.class)
public class StompPushAutoConfig {

    @Bean("pushStompRegistry")
    public StompRegistry pushStompRegistry(ObjectProvider<JsonMapper> jsonMapper,
                                           @Qualifier("clientOutboundChannel") ObjectProvider<MessageChannel> clientOutboundChannel) {
        final Supplier<MessageChannel> channel = clientOutboundChannel::getObject; //Risolto al primo invio: nessuna dipendenza dalla configurazione WebSocket in creazione
        return new StompRegistry(PushAutoConfig.jsonMapperOrDefault(jsonMapper), SingletonSupplier.of(channel));
    }

    @Bean("pushStompService")
    public StompService pushStompService(ObjectProvider<JsonMapper> jsonMapper, ObjectProvider<Validator> validator,
                                         StompProperties properties) {
        return new StompService(PushAutoConfig.jsonMapperOrDefault(jsonMapper), validator.getIfUnique(),
                StompDestinations.of(properties.destinationPrefix()));
    }

    @Bean("pushStompEventManager")
    public StompEventManager pushStompEventManager(StompRegistry registry) {
        return new StompEventManager(registry);
    }

    @Bean("pushStompConfigurer")
    StompPushConfigurer pushStompConfigurer(StompProperties properties, ObjectProvider<StompController<?>> controllers,
                                            ListableBeanFactory beanFactory) {
        return new StompPushConfigurer(properties, controllers, beanFactory);
    }
}
