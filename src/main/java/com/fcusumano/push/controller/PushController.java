package com.fcusumano.push.controller;

import com.fcusumano.push.service.PushService;
import org.springframework.beans.factory.annotation.Autowired;



/**
 * Classe astratta base per tutti i controller dedicati alla gestione delle
 * operazioni di Push Messaging.
 *
 * <p>
 * {@code PushController} fornisce l'infrastruttura comune per i controller
 * specializzati che integrano un {@link com.fcusumano.push.service.PushService},
 * facilitando l'accesso ai servizi di pubblicazione, gestione delle sottoscrizioni
 * e interazione con i canali push definiti nel sistema.
 * </p>
 *
 * <h2>Scopo</h2>
 * <p>
 * Questa classe funge da superclass per controller che devono interagire con
 * componenti di tipo {@code PushService}.
 * L'iniezione del servizio avviene tramite un metodo annotato con
 * {@link org.springframework.beans.factory.annotation.Autowired}, garantendo che
 * le istanze concrete ricevano automaticamente la loro dipendenza durante la fase
 * di inizializzazione del contesto Spring.
 * </p>
 *
 * <h2>Parametrizzazione</h2>
 * <p>
 * Il tipo generico {@code S} rappresenta una sottoclasse di
 * {@code PushService<?, ?>}.
 * Questo permette a ogni controller derivato di operare su un servizio specifico,
 * mantenendo la classe base flessibile e riutilizzabile.
 * </p>
 *
 * <h2>Funzionamento</h2>
 * <ul>
 *     <li>La proprietà {@code service} è protetta, così da essere accessibile
 *         alle classi figlie.</li>
 *     <li>Il metodo {@code setService(S service)} viene automaticamente invocato
 *         da Spring grazie a {@code @Autowired}.</li>
 *     <li>La classe non espone logica applicativa diretta, delegando il lavoro
 *         ai controller specializzati.</li>
 * </ul>
 *
 * <h2>Utilizzo</h2>
 * <pre>{@code
 * @RestController
 * public class NotificationController extends PushController<SseService> {
 *     ...
 * }
 * }</pre>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>La classe non è annotata come {@code @Component}: spetta alle sottoclassi
 *         dichiararsi come controller Spring (es. tramite {@code @RestController}).</li>
 *     <li>Il metodo di injection è protetto per permettere override avanzati
 *         nelle classi figlie, se necessario.</li>
 *     <li>La classe non gestisce eccezioni o flussi applicativi: ciò è delegato
 *         ai controller concreti.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
abstract class PushController<S extends PushService<?, ?>> {

    protected S service;

    @Autowired
    protected void setService(S service) {
        this.service = service;
    }
}
