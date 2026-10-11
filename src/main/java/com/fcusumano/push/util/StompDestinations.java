package com.fcusumano.push.util;

import com.fcusumano.push.channel.ChannelEnum;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.util.StringUtils;


/**
 * Destinazioni STOMP riservate alla libreria sotto un prefisso comune:
 * {@code {prefix}/{CHANNEL}} per le sottoscrizioni e {@code {prefix}/filter/{subscription-id}}
 * per la modifica dei filtri.
 * <p>
 * I topic dei canali sono in maiuscolo (verificato dall'annotation processor), quindi il segmento
 * {@code filter} non può mai coincidere con un topic.
 * </p>
 *
 * @param prefix prefisso delle destinazioni: inizia e non finisce con {@code /}
 *
 * @author Francesco Cusumano
 */
public record StompDestinations(@NotBlank String prefix) {

    private static final String FILTER_SEGMENT = "/filter/";

    /**
     * Verifica il prefisso.
     *
     * @throws IllegalArgumentException se il prefisso non inizia con {@code /}, finisce con {@code /} o è solo {@code /}
     */
    public StompDestinations {
        if (prefix == null || prefix.length() <= 1 || !prefix.startsWith("/") || prefix.endsWith("/")) {
            throw new IllegalArgumentException("push-manager.stomp.destination-prefix must start with '/' and must not end with '/': '" + prefix + "'");
        }
    }

    public static StompDestinations of(String prefix) {
        return new StompDestinations(prefix);
    }

    /**
     * @param destination destinazione di un frame (opzionale)
     * @return {@code true} se la destinazione è il prefisso o è sotto il prefisso
     */
    public boolean isPush(String destination) {
        return destination != null && (destination.equals(prefix) || destination.startsWith(prefix + "/"));
    }

    /**
     * @param destination destinazione di un frame (opzionale)
     * @return il topic di {@code {prefix}/{topic}}, {@code null} se la destinazione non ha questa forma
     */
    public String topic(String destination) {
        if (destination == null || !destination.startsWith(prefix + "/")) return null;
        final String topic = destination.substring(prefix.length() + 1);
        return topic.isEmpty() || topic.contains("/") ? null : topic;
    }

    /**
     * @param destination destinazione di un frame (opzionale)
     * @return l'id della sottoscrizione di {@code {prefix}/filter/{subscription-id}}, {@code null} se assente
     */
    public String filterSubscriptionId(String destination) {
        if (destination == null || !destination.startsWith(prefix + FILTER_SEGMENT)) return null;
        final String subscriptionId = destination.substring(prefix.length() + FILTER_SEGMENT.length());
        return StringUtils.hasText(subscriptionId) ? subscriptionId : null;
    }

    /** @return la destinazione delle sottoscrizioni al canale */
    public String channel(@NotNull ChannelEnum channel) {
        return prefix + "/" + channel.topic();
    }

    /** @return la destinazione per modificare i filtri della sottoscrizione */
    public String filter(@NotBlank String subscriptionId) {
        return prefix + FILTER_SEGMENT + subscriptionId;
    }
}
