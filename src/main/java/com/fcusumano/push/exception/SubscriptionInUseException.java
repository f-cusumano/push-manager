package com.fcusumano.push.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Id di sottoscrizione STOMP già in uso nella sessione, da un client registrato o da una sottoscrizione rifiutata.
 * STOMP non risponde via HTTP: lo status {@code 409} segue solo la convenzione del package.
 *
 * @author Francesco Cusumano
 */
@ResponseStatus(HttpStatus.CONFLICT)
public class SubscriptionInUseException extends RuntimeException {

    public SubscriptionInUseException(String message) {
        super(message);
    }

    public SubscriptionInUseException(String pattern, Object... args) {
        this(String.format(pattern, args));
    }
}
