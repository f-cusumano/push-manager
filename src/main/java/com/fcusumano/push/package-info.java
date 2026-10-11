/**
 * Libreria Spring Boot per la messaggistica push in tempo reale (SSE e STOMP su WebSocket).
 * <p>
 * Si attiva annotando la classe di avvio con
 * {@link com.fcusumano.push.annotation.EnablePushMessaging}: vedi il README del progetto.
 * STOMP si abilita con {@code @EnablePushMessaging(stomp = true)} e richiede spring-boot-starter-websocket.
 * </p>
 */
package com.fcusumano.push;
