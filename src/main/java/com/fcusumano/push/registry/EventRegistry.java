package com.fcusumano.push.registry;

import com.fcusumano.push.event.PushEvent;


/**
 * Interfaccia base per i registry dedicati alla gestione e correlazione degli
 * eventi {@link PushEvent} all'interno del sistema di Push Messaging.
 *
 * <p>
 * Un {@code EventRegistry} funge da punto di aggregazione per le operazioni
 * legate all'elaborazione o alla distribuzione di eventi di tipo {@code E},
 * lasciando alle implementazioni concrete la responsabilità di definire il
 * comportamento specifico. Non impone vincoli di metodo per consentire la
 * massima flessibilità e adattabilità a diversi protocolli o meccanismi di
 * trasporto.
 * </p>
 *
 * <h2>Scopo</h2>
 * <p>
 * L'interfaccia è volutamente minimale e attualmente rappresenta un
 * <em>marker interface</em> utilizzato per:
 * </p>
 * <ul>
 *     <li>tipizzare correttamente manager ed eventi ({@link com.fcusumano.push.event.EventManager});</li>
 *     <li>favorire l’inversione di controllo tramite dipendenze ben definite;</li>
 *     <li>aggregare responsabilità omogenee relative alla gestione degli eventi;</li>
 *     <li>permettere future evoluzioni dell’API senza rompere la compatibilità.</li>
 * </ul>
 *
 * <h2>Estendibilità</h2>
 * <p>
 * Le implementazioni concrete possono fornire funzionalità quali:
 * </p>
 * <ul>
 *     <li>memorizzazione degli eventi;</li>
 *     <li>gestione degli eventi in memoria o su storage esterno;</li>
 *     <li>funzionalità di broadcasting (come nel caso di {@code SseRegistry});</li>
 *     <li>meccanismi di correlazione, scheduling o retry;</li>
 *     <li>instradamento su protocolli diversi (SSE, WebSocket, STOMP, MQTT, ecc.).</li>
 * </ul>
 *
 * <h2>Integrazione con EventManager</h2>
 * <p>
 * Ogni {@code EventManager<E, R>} richiede un repository di tipo {@code R}
 * che implementa {@code EventRegistry<E>}.
 * Ciò consente alle diverse implementazioni di fornire logiche di
 * persistenza o distribuzione specifiche per ciascun tipo di evento.
 * </p>
 *
 * <h2>Prossime estensioni</h2>
 * <p>
 * L’interfaccia è progettata per essere estesa in futuro, ad esempio con
 * operazioni comuni come:
 * </p>
 * <ul>
 *     <li>metodi per registrare, recuperare o filtrare eventi;</li>
 *     <li>metodi per il tracciamento degli stati di consegna;</li>
 *     <li>API di supporto per protocolli diversi da SSE.</li>
 * </ul>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>Attualmente rappresenta un elemento aggregativo, come indicato nei commenti originali.</li>
 *     <li>Le implementazioni concrete devono garantire thread-safety se utilizzate in contesti concorrenti.</li>
 *     <li>È progettata per mantenere il sistema aperto a nuove tipologie di eventi
 *         e nuove modalità di distribuzione.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public interface EventRegistry<E extends PushEvent<?>> {}
