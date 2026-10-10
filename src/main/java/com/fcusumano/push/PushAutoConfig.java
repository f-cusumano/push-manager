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
 *     <li>le {@link PushProperties} con prefisso {@code push-manager}.</li>
 * </ul>
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
