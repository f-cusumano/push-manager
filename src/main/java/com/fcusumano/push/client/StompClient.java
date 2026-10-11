package com.fcusumano.push.client;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.filter.FilteringOptions;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import static com.fcusumano.push.client.ClientStatus.DISABLED;
import static com.fcusumano.push.client.ClientStatus.STREAMING;
import static com.fcusumano.push.client.ClientStatus.SUBSCRIBED;


/**
 * Client STOMP, controparte di {@link SseClient}: una sottoscrizione a un canale con i suoi filtri.
 * <p>
 * Nasce {@code SUBSCRIBED}, passa a {@code STREAMING} dopo l'invio dell'evento {@code CONNECTED}
 * e diventa {@code DISABLED} alla disiscrizione o alla chiusura della sessione.
 * Stato e filtri sono letti dal broadcast e modificati dai frame del client: le transizioni sono sincronizzate.
 * </p>
 *
 * @param <T> tipo dei payload del canale
 *
 * @author Francesco Cusumano
 */
public class StompClient<T> {

    private final @NotNull StompSubscription subscription;
    private final @NotNull ChannelEnum channel;
    private final @NotBlank String destination;
    private volatile @NotNull ClientStatus status; //Letto dal broadcast, scritto dai frame e dagli eventi di sessione
    private volatile FilteringOptions<T> filteringOptions;


    /**
     * @param subscription identità del client
     * @param channel canale sottoscritto
     * @param destination destinazione della sottoscrizione, riportata nei messaggi
     */
    public StompClient(@NotNull StompSubscription subscription, @NotNull ChannelEnum channel, @NotBlank String destination) {
        this.subscription = subscription;
        this.channel = channel;
        this.destination = destination;
        this.status = SUBSCRIBED;
    }

    public StompSubscription subscription() {
        return subscription;
    }

    public String sessionId() {
        return subscription.sessionId();
    }

    public String subscriptionId() {
        return subscription.subscriptionId();
    }

    public ChannelEnum channel() {
        return channel;
    }

    public String destination() {
        return destination;
    }

    public ClientStatus status() {
        return status;
    }

    public FilteringOptions<T> filteringOptions() {
        return filteringOptions;
    }

    public void setFilteringOptions(FilteringOptions<T> filteringOptions) {
        this.filteringOptions = filteringOptions;
    }

    /** @return classe dei payload del canale a cui il client è sottoscritto */
    @SuppressWarnings("unchecked")
    public Class<T> payloadType() {
        return (Class<T>) channel.payloadType();
    }

    /** @return {@code true} se il client è passato da {@code SUBSCRIBED} a {@code STREAMING} */
    public synchronized boolean activate() {
        if (status != SUBSCRIBED) return false;
        status = STREAMING;
        return true;
    }

    /** Disabilita il client: non riceve più eventi. */
    public synchronized void disable() {
        status = DISABLED;
    }

    @Override
    public String toString() {
        return subscription + " on '" + channel.topic() + "'";
    }
}
