package com.fcusumano.push.filter;

import com.fcusumano.push.event.EventType;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.function.Predicate;

/**
 * Richiesta di filtering a livello applicativo per gli eventi
 * nell'ambito della gestione degli eventi nel sistema di Push Messaging.
 *
 * @param allowedEventTypes filtri sul tipo di evento: se null/vuoto = nessun filtro applicato,
 *                          solo gli eventi specificati altrimenti;
 *                          gli eventi non muteable non possono essere esclusi
 * @param condition filtro sul contenuto dell'evento (composto da tutti i filtri richiesti):
 *                  se null = nessun filtro applicato, solo gli eventi conformi altrimenti
 *
 * @author Francesco Cusumano
 */
public record FilteringOptions<T>(
        List<@NotNull EventType> allowedEventTypes,
        Predicate<T> condition
) {
    /**
     * Crea le opzioni componendo in AND tutti i predicati sul contenuto.
     *
     * @param allowedEventTypes tipi di evento ammessi (null/vuoto = tutti)
     * @param conditions predicati sul contenuto (null/vuoto = nessun filtro)
     * @return le opzioni di filtraggio
     */
    public static <F> FilteringOptions<F> of(List<@NotNull EventType> allowedEventTypes,
                                             List<@NotNull Predicate<F>> conditions) {
        final Predicate<F> fullCondition = conditions == null || conditions.isEmpty() ? null
                : conditions.stream().reduce(Predicate::and).orElse(null);
        return new FilteringOptions<>(allowedEventTypes, fullCondition);
    }
}
