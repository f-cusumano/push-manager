package com.fcusumano.push;

import com.fcusumano.push.event.SseEventManager;
import com.fcusumano.push.properties.PushProperties;
import com.fcusumano.push.registry.SseRegistry;
import com.fcusumano.push.service.SseService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.event.TransactionalEventListenerFactory;
import tools.jackson.databind.json.JsonMapper;

/**
 * Auto-configurazione di push-manager.
 * <p>
 * Non è registrata in {@code AutoConfiguration.imports}: viene importata solo da
 * {@link com.fcusumano.push.annotation.EnablePushMessaging}, così la libreria si attiva
 * unicamente se l'applicazione lo chiede esplicitamente.
 * </p>
 * <p>Registra:</p>
 * <ul>
 *     <li>{@link SseRegistry}, il registro thread-safe dei client per canale;</li>
 *     <li>{@link SseService}, la logica di sottoscrizione, filtro, streaming e disiscrizione;</li>
 *     <li>{@link SseEventManager}, da iniettare nell'applicazione per pubblicare eventi;</li>
 *     <li>un {@link TaskScheduler} chiamato {@code taskScheduler} (2 thread, prefisso {@code push-scheduler-})
 *         usato per gli heartbeat;</li>
 *     <li>le {@link PushProperties} con prefisso {@code push-manager};</li>
 *     <li>una {@link TransactionalEventListenerFactory}, che rende attivi i {@code @TransactionalEventListener}
 *         anche se l'applicazione non abilita la gestione delle transazioni con {@code @EnableTransactionManagement}.</li>
 * </ul>
 * <p>
 * Registry e service usano il {@link JsonMapper} dell'applicazione, se presente, per convertire i valori
 * dei filtri e selezionare i campi dei payload con la stessa configurazione usata per serializzarli.
 * </p>
 * <p>
 * Abilita inoltre {@code @Async}, necessario alla consegna asincrona degli eventi
 * (vedi {@link com.fcusumano.push.event.EventManager#onEvent}).
 * </p>
 *
 * @author Francesco Cusumano
 */
@EnableAsync
@AutoConfiguration
@EnableConfigurationProperties(PushProperties.class)
public class PushAutoConfig {

    @Bean("sseRegistry")
    public SseRegistry sseRegistry(ObjectProvider<JsonMapper> jsonMapper) {
        return new SseRegistry(jsonMapperOrDefault(jsonMapper));
    }

    @Bean("sseService")
    public SseService sseService(ObjectProvider<JsonMapper> jsonMapper) {
        return new SseService(jsonMapperOrDefault(jsonMapper));
    }

    @Bean("sseEventManager")
    public SseEventManager sseEventManager(SseRegistry registry) {
        return new SseEventManager(registry);
    }

    //Senza factory i @TransactionalEventListener vengono ignorati e gli eventi non arrivano mai ai client
    @Bean("pushTransactionalEventListenerFactory")
    public static TransactionalEventListenerFactory pushTransactionalEventListenerFactory() {
        return new TransactionalEventListenerFactory();
    }

    @Bean("taskScheduler")
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("push-scheduler-");
        scheduler.initialize();
        return scheduler;
    }

    //Package-private: riusato da StompPushAutoConfig
    static JsonMapper jsonMapperOrDefault(ObjectProvider<JsonMapper> jsonMapper) {
        return jsonMapper.getIfAvailable(() -> JsonMapper.builder().build());
    }
}
