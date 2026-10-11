package com.fcusumano.push.service;

import com.fcusumano.push.properties.PushProperties;
import com.fcusumano.push.event.PushEvent;
import com.fcusumano.push.registry.EventRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;


/**
 * Classe astratta che fornisce la base per tutti i servizi di gestione degli eventi
 * nel sistema di Push Messaging. Un {@code PushService} coordina l’accesso alle
 * proprietà di configurazione, al registry degli eventi e al task scheduler,
 * offrendo alle implementazioni concrete un’infrastruttura condivisa e modulare.
 *
 * <p>
 * La classe è progettata per rappresentare lo strato applicativo principale per la
 * pubblicazione di eventi {@link PushEvent} e per l'interazione con un
 * {@link EventRegistry} specializzato. Le dipendenze vengono iniettate tramite
 * setter annotati con {@link org.springframework.beans.factory.annotation.Autowired}
 * per garantire un’inizializzazione ordinata nel ciclo di vita del bean Spring.
 * </p>
 *
 * <h2>Parametri generici</h2>
 * <ul>
 *     <li><b>E</b>: tipo dell’evento gestito, estensione di {@link PushEvent};</li>
 *     <li><b>R</b>: tipo del registry associato, implementazione di {@link EventRegistry}
 *         parametrizzata sullo stesso tipo di evento.</li>
 * </ul>
 *
 * <h2>Componenti forniti</h2>
 * <ul>
 *     <li><b>PushProperties</b>: configurazioni relative al sistema di push.</li>
 *     <li><b>EventRegistry</b>: archivio o gestore degli eventi, usato per
 *         la memorizzazione, il routing o la distribuzione.</li>
 *     <li><b>TaskScheduler</b>: scheduler Spring utilizzato per operazioni
 *         pianificate, retry asincroni o heartbeat periodici.</li>
 * </ul>
 *
 * <h2>Iniezione delle dipendenze</h2>
 * <p>
 * Le dipendenze principali vengono fornite tramite metodi setter contrassegnati da
 * {@code @Autowired}, seguendo il principio dell'injection-based lifecycle di Spring.
 * Ciò consente:
 * </p>
 * <ul>
 *     <li>una maggiore testabilità tramite injection esplicita;</li>
 *     <li>una corretta inizializzazione gerarchica nei servizi che estendono questa classe;</li>
 *     <li>la possibilità di sovrascrivere i setter nelle implementazioni concrete
 *         per aggiungere logica di setup personalizzata.</li>
 * </ul>
 *
 * <h2>Utilizzo</h2>
 * <p>
 * Le classi concrete devono estendere {@code PushService} e implementare la logica
 * di pubblicazione, manipolazione o gestione degli eventi, delegando al registry e
 * al task scheduler le attività operative.
 * </p>
 *
 * <h2>Esempio di implementazione</h2>
 * <pre>{@code
 * @Service
 * public class SseService extends PushService<SsePushEvent<?>, SseRegistry> {
 *     ...
 * }
 * }</pre>
 *
 * <h2>Estendibilità</h2>
 * <p>
 * {@code PushService} è progettato per essere esteso da servizi che necessitano di:
 * </p>
 * <ul>
 *     <li>gestire diversi tipi di eventi e protocolli di trasporto (SSE, WebSocket, STOMP...);</li>
 *     <li>coordinare la pubblicazione attraverso {@link EventRegistry};</li>
 *     <li>integrare politiche di retry tramite {@link TaskScheduler};</li>
 *     <li>accendere logiche domain-specific per ogni tipo di push event.</li>
 * </ul>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe non fornisce metodi astratti: sta alle implementazioni concrete
 *         definire le operazioni di invio o trasformazione degli eventi.</li>
 *     <li>È consigliato mantenere thread-safe eventuali logiche di aggregazione,
 *         soprattutto quando si interagisce con registry concorrenti.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public abstract class PushService<E extends PushEvent<?>, R extends EventRegistry<E>> {

    protected PushProperties properties;
    protected R registry;
    protected TaskScheduler taskScheduler;

    @Autowired
    protected void setProperties(PushProperties properties) {
        this.properties = properties;
    }

    @Autowired
    protected void setRegistry(R registry) {
        this.registry = registry;
    }

    @Autowired //Scheduler unico o @Primary, come senza qualifier; se è ambiguo (es. con STOMP c'è anche il 'messageBrokerTaskScheduler' di Spring) quello della libreria
    protected void setTaskScheduler(ObjectProvider<TaskScheduler> taskSchedulers,
                                    @Qualifier("taskScheduler") ObjectProvider<TaskScheduler> libraryTaskScheduler) {
        setTaskScheduler(taskSchedulers.getIfUnique(libraryTaskScheduler::getObject));
    }

    protected void setTaskScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }
}
