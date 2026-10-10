package com.fcusumano.push.filter;

import com.fcusumano.push.event.EventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.util.CollectionUtils;

import java.util.List;



/**
 * Richiesta di filtering a livello REST per gli eventi
 * nell'ambito della gestione degli eventi nel sistema di Push Messaging.
 *
 * @param requiredEventTypes filtri sul tipo di evento: se null/vuoto = nessun filtro applicato,
 *                           solo gli eventi specificati altrimenti;
 *                           gli eventi non muteable non possono essere esclusi
 * @param contentFilters filtri sul contenuto dell'evento: se null/vuoto = nessun filtro applicato,
 *                       solo gli eventi conformi altrimenti
 *
 * @author Francesco Cusumano
 */
public record FilteringRequest(
        List<@NotNull EventType> requiredEventTypes,
        List<@NotNull @Valid StreamingFilterRequest> contentFilters
) {

    public static boolean nullEquivalent(FilteringRequest request) {
        if (request == null) return true;
        return CollectionUtils.isEmpty(request.requiredEventTypes) && CollectionUtils.isEmpty(request.contentFilters);
    }
}
