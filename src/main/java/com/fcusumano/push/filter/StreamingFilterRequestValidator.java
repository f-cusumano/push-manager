package com.fcusumano.push.filter;

import com.fcusumano.push.util.AbstractValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.springframework.util.StringUtils;

import static com.fcusumano.push.filter.TargetDefinition.SINGLE_OBJECT;


/**
 * Validatore custom per la richiesta di filtraggio applicata alle connessioni
 * di streaming SSE, rappresentata da {@link StreamingFilterRequest}.
 *
 * <p>
 * {@code StreamingFilterRequestValidator} estende {@link AbstractValidator} ed è
 * responsabile della validazione semantica delle regole di filtro definite dal client,
 * assicurando la coerenza tra:
 * </p>
 * <ul>
 *     <li>il tipo di controllo ({@link CheckType});</li>
 *     <li>la definizione del target ({@link TargetDefinition});</li>
 *     <li>i parametri opzionali {@code value} e {@code fieldPathValue}.</li>
 * </ul>
 *
 * <h2>Obiettivo della validazione</h2>
 * <p>
 * Il validatore garantisce che la combinazione dei campi della request sia coerente
 * con il tipo di controllo richiesto, prevenendo configurazioni ambigue o semanticamente
 * scorrette prima che la logica applicativa venga eseguita.
 * </p>
 *
 * <h2>Comportamento di validazione</h2>
 * <p>
 * Il metodo {@link #isValid(StreamingFilterRequest, ConstraintValidatorContext)}
 * applica le seguenti regole:
 * </p>
 *
 * <h3>1. Request nulla</h3>
 * Una richiesta {@code null} è considerata valida.
 * Ciò consente di rendere il filtro opzionale.
 *
 * <h3>2. CheckType con TargetDefinition = SINGLE_OBJECT</h3>
 * <p>
 * Quando il {@link CheckType} opera su un singolo oggetto
 * ({@link TargetDefinition#SINGLE_OBJECT}):
 * </p>
 * <ul>
 *     <li>{@code value} deve essere assente;</li>
 *     <li>{@code fieldPathValue} deve essere assente;</li>
 *     <li>qualunque parametro aggiuntivo viene considerato non valido.</li>
 * </ul>
 *
 * <h3>3. CheckType che richiede un secondo termine di confronto</h3>
 * <p>Quando il {@link TargetDefinition} non è {@code SINGLE_OBJECT}:</p>
 * <ul>
 *     <li>deve essere valorizzato <b>almeno uno</b> tra {@code value} e {@code fieldPathValue};</li>
 *     <li>deve essere valorizzato <b>esattamente uno</b> dei due;</li>
 *     <li>la presenza contemporanea di entrambi è considerata non valida.</li>
 * </ul>
 *
 * <h3>4. Tipi di confronto</h3>
 * <p>
 * La validazione del tipo dei valori a confronto (compatibilità tra campo e valore)
 * <b>non è responsabilità di questo validatore</b> ed è demandata al livello applicativo.
 * </p>
 *
 * <h2>Messaggi di errore</h2>
 * <p>
 * In caso di violazione delle regole, il validatore utilizza il
 * {@link ConstraintValidatorContext} per registrare messaggi di errore chiari
 * e specifici, includendo il {@link CheckType} responsabile della violazione.
 * </p>
 *
 * <h2>Note</h2>
 * <ul>
 *     <li>Questa classe non valida il contenuto semantico del filtro, ma solo la
 *         coerenza strutturale.</li>
 *     <li>È progettata per essere riutilizzata in contesti di streaming e filtering
 *         real-time.</li>
 * </ul>
 *
 * @author Francesco Cusumano
 */
public class StreamingFilterRequestValidator
        extends AbstractValidator<ValidateStreamingFilterRequest, StreamingFilterRequest> {

    @Override
    public boolean isValid(StreamingFilterRequest filter, ConstraintValidatorContext context) {
        if (filter == null) return true;
        final CheckType checkType = filter.check();
        final TargetDefinition targetDefinition = checkType.getTargetDefinition();
        final boolean valuePresent = filter.value() != null;
        final boolean pathValuePresent = StringUtils.hasText(filter.fieldPathValue());

        //Se il check coinvolge un solo elemento, gli altri due campi non devono essere valorizzati
        if (targetDefinition == SINGLE_OBJECT) {
            return (!valuePresent && !pathValuePresent)
                   || validationError(context, "CheckType 'SINGLE_OBJECT' doesn't need a second argument to check.");
        }

        //Altrimenti se il check prevede un secondo termine di confronto...

        //..deve essere presente almeno uno tra 'value' and 'fieldPathValue'..
        if (!valuePresent && !pathValuePresent) {
            return validationError(context,
                    "At least one between 'value' and 'fieldPathValue' must be present for CheckType '%s'.",
                    checkType.name());
        }
        //..e precisamente solo uno dei due
        if (valuePresent && pathValuePresent) {
            return validationError(context,
                    "Both 'value' and 'fieldPathValue' present for CheckType '%s'.",
                    checkType.name());
        }

        //Le validazioni tra i tipi a confronto sono lasciate alla parte applicativa.
        return true;
    }
}
