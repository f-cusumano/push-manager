package com.fcusumano.push.util;

/**
 * Costanti di progetto.
 */
public final class Constant {

    private Constant() {}

    /**
     * Carattere di separazione nei path per property annidate.
     * In assenza di separatori ci si riferisce al campo diretto dell'oggetto,
     * es:  il contenuto di 'field1' per 'obj' è accessibile tramite 'obj.getField1()'.
     * In presenza di separatori ci si riferisce al percorso per raggiungere il campo,
     * es:  il contenuto di 'field1.field2.field3' per 'obj' è accessibile tramite
     *      'obj..getField1().getField2().getField3()'.
     */
    public static final String NESTED_PROPERTY_SEPARATOR = ".";
}
