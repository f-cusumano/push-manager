package com.fcusumano.push.annotation;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import java.util.List;
import java.util.Map;


/**
 * Importa la configurazione STOMP solo con {@code @EnablePushMessaging(stomp = true)}.
 * <p>
 * Nomina le classi come stringhe, così senza spring-websocket e spring-messaging nel classpath
 * nessuna classe di messaging viene caricata. Non è un {@code DeferredImportSelector}: l'infrastruttura
 * WebSocket viene registrata insieme alla configurazione dell'applicazione, prima delle auto-configurazioni di Boot.
 * </p>
 *
 * @author Francesco Cusumano
 */
final class StompImportSelector implements ImportSelector, BeanClassLoaderAware {

    static final String STOMP_AUTO_CONFIG = "com.fcusumano.push.StompPushAutoConfig"; //Per nome: nessuna classe di messaging caricata
    private static final List<String> STOMP_REQUIRED_CLASSES = List.of(
            "org.springframework.messaging.simp.stomp.StompHeaderAccessor",
            "org.springframework.web.socket.messaging.StompSubProtocolHandler");

    private ClassLoader classLoader;


    @Override
    public void setBeanClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public String[] selectImports(AnnotationMetadata metadata) {
        final Map<String, Object> attributes = metadata.getAnnotationAttributes(EnablePushMessaging.class.getName());
        if (attributes == null || !Boolean.TRUE.equals(attributes.get("stomp"))) return new String[0];
        for (String requiredClass : STOMP_REQUIRED_CLASSES) {
            if (!ClassUtils.isPresent(requiredClass, classLoader)) {
                throw new IllegalStateException("@EnablePushMessaging(stomp = true) requires spring-websocket and spring-messaging: add spring-boot-starter-websocket");
            }
        }
        return new String[] {STOMP_AUTO_CONFIG};
    }
}
