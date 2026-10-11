package com.fcusumano.push.filter;

import com.fcusumano.push.exception.InvalidFilterException;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.core.ResolvableType;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fcusumano.push.util.Constant.NESTED_PROPERTY_SEPARATOR;


/**
 * Selezione dei soli campi di interesse di un payload, richiesta dal client con
 * {@link FilteringRequest#requiredFieldsPath()}.
 *
 * <p>
 * I percorsi usano il punto per i campi annidati ({@code address.city}) e attraversano
 * collezioni e array applicandosi a ogni elemento ({@code orders.amount}).
 * Un campo selezionato porta con sé tutti i suoi sottocampi: selezionare {@code address}
 * include anche {@code address.city}.
 * </p>
 *
 * <p>
 * La selezione lavora sull'albero JSON del payload, così rispetta la configurazione Jackson
 * dell'applicazione; i percorsi vengono verificati una volta sola sui campi della classe del payload,
 * quando il client li richiede. Se tutti i campi selezionati sono vuoti (null, collezioni vuote
 * o oggetti con soli campi vuoti) {@link #select(JsonNode)} restituisce {@code null} e l'evento
 * non viene inviato al client.
 * </p>
 *
 * <p>L'istanza è immutabile dopo la costruzione e può essere usata da più thread.</p>
 *
 * @author Francesco Cusumano
 */
public final class FieldSelector {

    private final Map<String, FieldSelector> children = new HashMap<>();
    private boolean wholeField; //Campo selezionato con tutti i sottocampi

    private FieldSelector() {
    }

    /**
     * Crea la selezione dei campi verificando i percorsi sulla classe del payload.
     *
     * @param type <i>(obbligatorio)</i> la classe del payload
     * @param fieldsPath <i>(opzionale)</i> i percorsi dei campi da selezionare
     * @return {@code null} se non è richiesto alcun campo (payload intero), la selezione altrimenti
     * @throws InvalidFilterException se un percorso non corrisponde ai campi della classe
     */
    public static FieldSelector of(@NotNull Class<?> type, List<@NotBlank String> fieldsPath) {
        if (CollectionUtils.isEmpty(fieldsPath)) return null;
        final FieldSelector root = new FieldSelector();
        for (String fieldPath : fieldsPath) {
            final String[] pathCrumbles = fieldPath.split("\\" + NESTED_PROPERTY_SEPARATOR);
            checkPath(type, fieldPath, pathCrumbles);
            root.add(pathCrumbles);
        }
        return root;
    }

    /**
     * Applica la selezione all'albero JSON del payload.
     *
     * @param source <i>(obbligatorio)</i> l'albero JSON del payload, condiviso tra i client e non modificato
     * @return i soli campi selezionati, {@code null} se sono tutti vuoti
     */
    public JsonNode select(@NotNull JsonNode source) {
        final JsonNode result = project(source);
        return isEmpty(result) ? null : result;
    }

    private void add(String[] pathCrumbles) {
        FieldSelector node = this;
        for (String pathCrumble : pathCrumbles) {
            if (node.wholeField) return; //Già selezionato un campo padre, che include anche questo
            node = node.children.computeIfAbsent(pathCrumble, _ -> new FieldSelector());
        }
        node.wholeField = true;
        node.children.clear(); //Il campo intero include i sottocampi eventualmente già richiesti
    }

    private JsonNode project(JsonNode source) {
        if (wholeField || source == null || source.isNull()) return source;
        if (source.isArray()) { //La selezione vale per ogni elemento
            final ArrayNode result = JsonNodeFactory.instance.arrayNode();
            for (JsonNode element : source) {
                final JsonNode projected = project(element);
                if (projected == null) result.addNull(); else result.add(projected);
            }
            return result;
        }
        if (!source.isObject()) return null; //Sottocampi richiesti su un valore semplice
        final ObjectNode result = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, FieldSelector> child : children.entrySet()) {
            final JsonNode projected = child.getValue().project(source.get(child.getKey()));
            if (projected == null) result.putNull(child.getKey()); else result.set(child.getKey(), projected);
        }
        return result;
    }

    //Vuoto = null, collezione con soli elementi vuoti o oggetto con soli campi vuoti; 0, false e "" sono dati
    static boolean isEmpty(JsonNode node) {
        if (node == null || node.isNull()) return true;
        if (!node.isArray() && !node.isObject()) return false;
        for (JsonNode child : node) {
            if (!isEmpty(child)) return false;
        }
        return true;
    }

    //Verifica il percorso sui campi della classe, entrando negli elementi di collezioni e array
    private static void checkPath(@NotNull Class<?> type, @NotBlank String fieldPath, String[] pathCrumbles) {
        if (pathCrumbles.length == 0) {
            throw new InvalidFilterException("'%s' is not a valid path for %s", fieldPath, type.getSimpleName());
        }
        ResolvableType currentType = ResolvableType.forClass(type);
        for (String pathCrumble : pathCrumbles) {
            currentType = elementType(currentType);
            final Class<?> currentClass = currentType.resolve();
            //Tipo generico o mappa: i nomi dei sottocampi non sono verificabili
            if (currentClass == null || currentClass == Object.class || Map.class.isAssignableFrom(currentClass)) return;
            if (BeanUtils.isSimpleValueType(currentClass)) {
                throw new InvalidFilterException("'%s' has no subfields in path '%s'", currentClass.getSimpleName(), fieldPath);
            }
            final Field field = ReflectionUtils.findField(currentClass, pathCrumble);
            if (field == null) {
                throw new InvalidFilterException("'%s' is not a valid field for %s in path '%s'",
                        pathCrumble, currentClass.getSimpleName(), fieldPath);
            }
            currentType = ResolvableType.forField(field, currentType);
        }
    }

    private static ResolvableType elementType(ResolvableType type) {
        ResolvableType result = type;
        while (result.isArray() || Collection.class.isAssignableFrom(result.toClass())) {
            result = result.isArray() ? result.getComponentType() : result.asCollection().getGeneric(0);
        }
        return result;
    }
}
