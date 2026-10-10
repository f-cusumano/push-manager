package com.fcusumano.push.controller;

import com.fcusumano.push.channel.ChannelEnum;
import com.fcusumano.push.filter.FilteringRequest;
import com.fcusumano.push.service.SseService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;


/**
 * Controller astratto che fornisce gli endpoint REST per la gestione delle
 * connessioni Server-Sent Events (SSE) e delle relative funzionalità di
 * sottoscrizione, filtraggio, streaming e disconnessione.
 *
 * <p>
 * Le implementazioni concrete di questa classe devono definire il mapping
 * del controller (ad esempio tramite {@code @RequestMapping}) e possono
 * specializzarsi su uno specifico tipo di {@link ChannelEnum}.
 * </p>
 *
 * <h2>Funzionalità principali</h2>
 * <ul>
 *     <li>Gestione della sottoscrizione a un canale SSE.</li>
 *     <li>Applicazione o aggiornamento dinamico dei criteri di filtraggio degli eventi.</li>
 *     <li>Avvio dello streaming SSE verso il client.</li>
 *     <li>Gestione della disiscrizione del client.</li>
 * </ul>
 *
 * <h2>Generics</h2>
 * <p>
 * Il tipo {@code C} rappresenta una specifica enum che implementa
 * {@link ChannelEnum} e identifica il set dei canali sottoscrivibili.
 * </p>
 *
 * <h2>Integrazione</h2>
 * <p>
 * La classe eredita da {@link PushController}, ottenendo automaticamente
 * un'istanza di {@link SseService} tramite injection Spring.
 * Le implementazioni di questo controller non contengono logica applicativa,
 * ma delegano al servizio SSE la gestione dei client e dello streaming.
 * </p>
 *
 * <h2>Esempio di controller concreto</h2>
 * <pre>{@code
 * @RestController
 * @RequestMapping("/users")
 * public class UserController extends SseController<MyChannels> {
 *     ...
 * }
 * }</pre>
 *
 * @author Francesco Cusumano
 */
@Validated
public abstract class SseController<C extends ChannelEnum> extends PushController<SseService> {

    /**
     * Sottoscrive un client SSE a un canale specificato.
     *
     * @param channel canale a cui sottoscriversi
     * @param filters eventuali filtri iniziali applicati allo stream
     * @return informazioni sulla sessione SSE creata
     */
    @Operation(
            summary = "Sottoscrizione SSE",
            description = "Crea una nuova sessione SSE per il canale specificato e restituisce le informazioni di sottoscrizione.",
            parameters = {
                    @Parameter(
                            name = "channel",
                            description = "Canale SSE a cui sottoscriversi",
                            required = true
                            //Schema commentato per offrire l'enum su Swagger: non c'è possibilità di farlo altrimenti
                            //, schema = @Schema(implementation = String.class)
                    )
            },
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    required = false,
                    description = "Opzionali filtri iniziali da applicare allo stream",
                    content = @Content(schema = @Schema(implementation = FilteringRequest.class))
            ),
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Sottoscrizione creata",
                            content = @Content(schema = @Schema(implementation = SseSubscriptionResponse.class))
                    ),
                    @ApiResponse(responseCode = "400", description = "Richiesta non valida")
            }
    )
    @PostMapping(value = "/sse/subscribe/{channel}", produces = MediaType.APPLICATION_JSON_VALUE)
    public SseSubscriptionResponse subscribe(
            @PathVariable("channel") C channel,
            @Valid @RequestBody(required = false) FilteringRequest filters) {
        return service.subscribe(channel, filters);
    }

    /**
     * Aggiorna le opzioni di filtraggio per una sessione SSE esistente.
     *
     * @param sessionId identificativo della sessione da aggiornare
     * @param filters filtri aggiornati, opzionali
     */
    @Operation(
            summary = "Aggiorna filtri SSE",
            description = "Aggiorna i filtri applicati alla sessione SSE specificata.",
            parameters = {
                    @Parameter(
                            name = "session-id",
                            description = "Identificativo della sessione SSE",
                            required = true,
                            schema = @Schema(type = "string")
                    )
            },
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    required = false,
                    description = "Filtri aggiornati",
                    content = @Content(schema = @Schema(implementation = FilteringRequest.class))
            ),
            responses = {
                    @ApiResponse(responseCode = "204", description = "Filtri aggiornati"),
                    @ApiResponse(responseCode = "404", description = "Sessione non trovata")
            }
    )
    @PutMapping(value = "/sse/filter/{session-id}")
    public void filter(@NotBlank @PathVariable("session-id") String sessionId,
                       @Valid @RequestBody(required = false) FilteringRequest filters) {
        service.updateFilters(sessionId, filters);
    }

    /**
     * Avvia lo streaming SSE per il client identificato da session-id.
     *
     * @param sessionId identificativo della sessione SSE
     * @return un {@link SseEmitter} configurato per lo stream degli eventi
     */
    @Operation(
            summary = "Avvia lo streaming SSE",
            description = "Apre la connessione SSE per la sessione indicata.",
            parameters = {
                    @Parameter(
                            name = "session-id",
                            description = "Identificativo della sessione SSE",
                            required = true,
                            schema = @Schema(type = "string")
                    )
            },
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Connessione SSE avviata",
                            content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE)
                    ),
                    @ApiResponse(responseCode = "404", description = "Sessione non trovata")
            }
    )
    @GetMapping(value = "/sse/stream/{session-id}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> startStreaming(@NotBlank @PathVariable("session-id") String sessionId) {
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .header("X-Accel-Buffering", "no")
                .header("Cache-Control", "no-cache")
                .header("Connection", "keep-alive")
                .body(service.startStreaming(sessionId));
    }

    /**
     * Disiscrive il client SSE e chiude la sessione associata.
     *
     * @param sessionId identificativo della sessione SSE da chiudere
     */
    @Operation(
            summary = "Disiscrizione SSE",
            description = "Disconnette il client SSE e chiude la sessione.",
            parameters = {
                    @Parameter(
                            name = "session-id",
                            description = "Identificativo della sessione SSE",
                            required = true,
                            schema = @Schema(type = "string")
                    )
            },
            responses = {
                    @ApiResponse(responseCode = "204", description = "Disiscrizione completata"),
                    @ApiResponse(responseCode = "404", description = "Sessione non trovata")
            }
    )
    @DeleteMapping(value = "/sse/unsubscribe/{session-id}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public void unsubscribe(@NotBlank @PathVariable("session-id") String sessionId) {
        service.unsubscribe(sessionId);
    }
}
