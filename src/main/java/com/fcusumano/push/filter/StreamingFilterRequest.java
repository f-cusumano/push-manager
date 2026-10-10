package com.fcusumano.push.filter;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;


/**
 * Richiesta di filtering a livello REST per il contenuto degli eventi
 * nell'ambito della gestione degli eventi nel sistema di Push Messaging.
 *
 * @param fieldPath il path del campo da considerare per l'esecuzione del check;
 *                  navigazione di oggetti complessi attraverso il '.',
 *                  es: "user.name" = il campo "name" all'interno del campo oggetto complesso "user"
 * @param check tipo di check da eseguire
 * @param value <i>(opzionale)</i> l'eventuale secondo argomento per il check
 * @param fieldPathValue <i>(opzionale)</i> l'eventuale path del campo del secondo argomento per il check
 *
 * NOTA: <b>value</b> e <b>fieldPathValue</b> se presenti sono mutuamente esclusivi.
 *
 * @author Francesco Cusumano
 */
@ValidateStreamingFilterRequest
public record StreamingFilterRequest(
        @NotBlank String fieldPath,
        @NotNull CheckType check,
        Object value, //Se presente/previsto, verrà deserializzato nel tipo del campo 'fieldPath'
        String fieldPathValue //Come value, ma valore preso da un altro campo
) {}
