package com.fcusumano.push.filter;

import com.fcusumano.push.event.EventType;
import com.fcusumano.push.event.PushEvent;
import jakarta.validation.constraints.NotNull;
import org.springframework.util.CollectionUtils;

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
 * @param fieldSelector campi del payload da trasmettere: se null = payload intero
 *
 * @author Francesco Cusumano
 */
public record FilteringOptions<T>(
        List<@NotNull EventType> allowedEventTypes,
        Predicate<T> condition,
        FieldSelector fieldSelector
) {
    /**
     * Crea le opzioni componendo in AND tutti i predicati sul contenuto.
     *
     * @param allowedEventTypes tipi di evento ammessi (null/vuoto = tutti)
     * @param conditions predicati sul contenuto (null/vuoto = nessun filtro)
     * @param fieldSelector campi del payload da trasmettere (null = payload intero)
     * @return le opzioni di filtraggio
     */
    public static <F> FilteringOptions<F> of(List<@NotNull EventType> allowedEventTypes,
                                             List<@NotNull Predicate<F>> conditions,
                                             FieldSelector fieldSelector) {
        final Predicate<F> fullCondition = conditions == null || conditions.isEmpty() ? null
                : conditions.stream().reduce(Predicate::and).orElse(null);
        return new FilteringOptions<>(allowedEventTypes, fullCondition, fieldSelector);
    }

    /**
     * Verifica se l'evento supera i filtri sul tipo (gli eventi non muteable sono sempre ammessi)
     * e sul contenuto.
     *
     * @param event l'evento da verificare
     * @return {@code true} se l'evento va inviato al client
     */
    public boolean accepts(@NotNull PushEvent<T> event) { //Stessa regola di SseRegistry.clientInterested
        final boolean eventPermitted = CollectionUtils.isEmpty(allowedEventTypes)
                                       || allowedEventTypes.contains(event.getEventType())
                                       || !event.getEventType().isMuteable();
        return eventPermitted && (condition == null || condition.test(event.getData()));
    }
}
