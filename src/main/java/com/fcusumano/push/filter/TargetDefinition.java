package com.fcusumano.push.filter;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Definisce il tipo di target logico su cui viene applicata una regola di
 * filtraggio all'interno del sistema di streaming e validazione eventi.
 *
 * <p>{@code TargetDefinition} è un attributo di {@link CheckType} che descrive:</p>
 * <ul>
 *     <li>il numero di elementi coinvolti nel confronto;</li>
 *     <li>la relazione di tipo tra gli elementi confrontati;</li>
 * </ul>
 *
 * <p>
 * L'enum è annotato con {@link io.swagger.v3.oas.annotations.media.Schema}
 * per consentire a Swagger/OpenAPI di trattare i valori come riferimento
 * enumerativo nella documentazione delle API.
 * </p>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>L’enum non definisce direttamente la logica di confronto,
 *         ma ne descrive esclusivamente la struttura.</li>
 *     <li>La validazione dei tipi effettivi è demandata al livello applicativo.</li>
 *     <li>È pensata per essere estesa implicitamente tramite nuovi {@link CheckType},
 *         non tramite nuovi valori dell’enum.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
@Schema(enumAsRef = true)
public enum TargetDefinition {

    /**
     * La regola opera su un singolo oggetto senza richiedere un secondo termine di confronto.
     * Tipico per controlli di esistenza, stato o presenza.
     */
    SINGLE_OBJECT,

    /**
     * La regola confronta due valori dello stesso tipo, ad esempio un campo dell’oggetto
     * con un valore costante o con un altro campo dello stesso tipo.
     */
    TWO_OBJ_SAME_TYPE,

    /**
     * La regola prevede il confronto tra due elementi di tipo differente,
     * tipicamente un campo dell’oggetto e un valore derivato o trasformato.
     */
    TWO_OBJ_DIFFERENT_TYPE
}
