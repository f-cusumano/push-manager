package com.fcusumano.push.filter;

import com.fcusumano.push.event.EventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
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
 * @param requiredFieldsPath campi del payload da trasmettere (con tutti i loro sottocampi): se null/vuoto =
 *                           payload intero, solo i campi indicati altrimenti; se sono tutti vuoti
 *                           l'evento non viene inviato
 *
 * @author Francesco Cusumano
 */
public record FilteringRequest(
        List<@NotNull EventType> requiredEventTypes,
        List<@NotNull @Valid StreamingFilterRequest> contentFilters,
        List<@NotBlank String> requiredFieldsPath
) {

    /**
     * @param request richiesta di filtraggio
     * @return {@code true} se la richiesta è nulla o non contiene alcun filtro
     */
    public static boolean nullEquivalent(FilteringRequest request) {
        if (request == null) return true;
        return CollectionUtils.isEmpty(request.requiredEventTypes) && CollectionUtils.isEmpty(request.contentFilters)
               && CollectionUtils.isEmpty(request.requiredFieldsPath);
    }
}
