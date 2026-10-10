package com.fcusumano.push.annotation;


import com.google.auto.service.AutoService;
import com.fcusumano.push.channel.ChannelDefinition;
import com.fcusumano.push.channel.ChannelEnum;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.javapoet.AnnotationSpec;
import org.springframework.javapoet.ClassName;
import org.springframework.javapoet.FieldSpec;
import org.springframework.javapoet.JavaFile;
import org.springframework.javapoet.MethodSpec;
import org.springframework.javapoet.TypeSpec;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.MirroredTypeException;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * Annotation Processor responsabile della generazione automatica della enum dei canali
 * definiti tramite l'annotation {@link com.fcusumano.push.annotation.EnablePushMessaging}.
 *
 * <p>
 * Il processor intercetta tutte le classi annotate con
 * {@link com.fcusumano.push.annotation.EnablePushMessaging} e genera dinamicamente
 * una enum che implementa {@link com.fcusumano.push.channel.ChannelEnum}, contenente
 * i canali dichiarati tramite le istanze di {@link com.fcusumano.push.channel.ChannelDefinition}.
 * </p>
 *
 * <h2>Funzionalità principali</h2>
 * <ul>
 *     <li>Analizza le annotation {@code @EnablePushMessaging} durante la compilazione.</li>
 *     <li>Genera automaticamente una enum contenente:
 *         <ul>
 *             <li>Un valore costante per ogni {@link ChannelDefinition} definito.</li>
 *             <li>Costruttore interno che accetta il payload type.</li>
 *             <li>Implementazioni dei metodi {@code topic()} e {@code payloadType()} dell’interfaccia {@link ChannelEnum}.</li>
 *         </ul>
 *     </li>
 *     <li>Validazione dei nomi dei topic secondo la convenzione UPPERCASE_SNAKE_CASE.</li>
 *     <li>Gestione corretta dei type mirror tramite {@link MirroredTypeException}, necessaria nei processor.</li>
 * </ul>
 *
 * <h2>Regole di validazione</h2>
 * <ul>
 *     <li>Ogni topic deve rispettare il pattern:
 *         <code>^[A-Z][A-Z0-9_]*$</code></li>
 *     <li>Se un topic non è valido, la compilazione viene interrotta con errore.</li>
 *     <li>Se la generazione della enum fallisce, il compilatore segnala un errore e blocca il build.</li>
 * </ul>
 *
 * <h2>Generazione della enum</h2>
 * <p>
 * Il processor utilizza la libreria <strong>JavaPoet</strong> per generare dinamicamente:
 * </p>
 * <ul>
 *     <li>La dichiarazione della enum.</li>
 *     <li>I campi e costruttori necessari.</li>
 *     <li>Le costanti corrispondenti ai canali.</li>
 *     <li>La scrittura del file sorgente nella directory di output del compilatore.</li>
 * </ul>
 *
 * <h2>Esempio di utilizzo dell’annotation da parte dell’utente</h2>
 * <pre>{@code
 * @EnablePushMessaging(
 *     channelEnumName = "MyChannels",
 *     channelCatalog = {
 *         @ChannelDefinition(topic = "ANGRAFICA_USER", payloadType = User.class),
 *         @ChannelDefinition(topic = "CONNECTED_USERS", payloadType = Integer.class)
 *     }
 * )
 * @SpringBootApplication
 * public class MyApplication {
 * }
 * }</pre>
 *
 * <h2>Limitazioni</h2>
 * <ul>
 *     <li>Il processor opera solo a compile-time e non durante il runtime.</li>
 *     <li>I tipi passati come payload devono essere disponibili nel classpath di compilazione.</li>
 *     <li>Il nome della enum generata non deve già esistere nel package target.</li>
 * </ul>
 *
 * <h2>Implementazione</h2>
 * <p>
 * Il processor è registrato tramite {@link com.google.auto.service.AutoService}, e supporta la
 * versione di linguaggio specificata da {@link SupportedSourceVersion}. Le annotation supportate
 * sono dichiarate tramite {@link SupportedAnnotationTypes}.
 * </p>
 *
 * @author Francesco Cusumano
 */
@AutoService(Processor.class)
@SupportedAnnotationTypes("com.fcusumano.push.annotation.EnablePushMessaging")
@SupportedSourceVersion(SourceVersion.RELEASE_25)
public class EnablePushMessagingProcessor extends AbstractProcessor {

    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]*$");

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element element : roundEnv.getElementsAnnotatedWith(EnablePushMessaging.class)) {
            final EnablePushMessaging annotation = element.getAnnotation(EnablePushMessaging.class);
            final String packageName = processingEnv.getElementUtils().getPackageOf(element).getQualifiedName().toString();
            final String enumName = annotation.channelEnumName();

            //Se fallisce la generazione degli enum o della configurazione push blocco il processo
            if (!generateChannelEnum(packageName, enumName, annotation.channelCatalog())) return true;
        }
        return true;
    }

    private boolean generateChannelEnum(@NotBlank String packageName, @NotBlank String enumName,
                                        @NotNull ChannelDefinition[] channels) {
        final TypeSpec.Builder enumBuilder = TypeSpec.enumBuilder(enumName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(AnnotationSpec.builder(Schema.class).addMember("enumAsRef", "true").build())
                .addSuperinterface(ChannelEnum.class);

        // Campi
        final FieldSpec payloadTypeField = FieldSpec.builder(Class.class, "payloadType", Modifier.PRIVATE, Modifier.FINAL).build();
        enumBuilder.addField(payloadTypeField);

        // Costruttore
        enumBuilder.addMethod(MethodSpec.constructorBuilder()
                .addParameter(Class.class, "payloadType")
                .addStatement("this.payloadType = payloadType")
                .build()
        );

        // Metodi topic() e payloadType()
        enumBuilder.addMethod(MethodSpec.methodBuilder("topic")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(String.class)
                .addStatement("return name()")
                .build()
        );
        enumBuilder.addMethod(MethodSpec.methodBuilder("payloadType")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(Class.class)
                .addStatement("return payloadType")
                .build()
        );

        // Aggiunta dei costanti enum
        for (ChannelDefinition def : channels) {
            final String topic = def.topic();
            if (!TOPIC_PATTERN.matcher(topic).matches()) {
                return error("Invalid topic '" + topic + "' " + ": Topic names must be UPPERCASE_SNAKE_CASE.");
            }

            ClassName payloadClassName;
            try {
                // Questo tenta di leggere Class<?> ma in un processor NON è mai possibile
                payloadClassName = ClassName.get(def.payloadType());
            } catch (MirroredTypeException mte) {
                // Questo è il ramo corretto: estrai il TypeMirror
                var typeMirror = mte.getTypeMirror();
                payloadClassName = ClassName.get((TypeElement) processingEnv.getTypeUtils().asElement(typeMirror));
            }

            enumBuilder.addEnumConstant(topic,
                    TypeSpec.anonymousClassBuilder("$T.class", payloadClassName).build());
        }

        // Scrittura file enum
        try {
            final JavaFile javaFile = JavaFile.builder(packageName, enumBuilder.build()).build();
            javaFile.writeTo(processingEnv.getFiler());
        } catch (IOException e) {
            return error("Error generating enum " + enumName + ": " + e.getMessage());
        }
        return true;
    }

    private boolean error(String msg) {
        processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, msg);
        return false;
    }
}
