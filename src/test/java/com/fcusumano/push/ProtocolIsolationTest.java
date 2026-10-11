package com.fcusumano.push;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.constantpool.PoolEntry;
import java.lang.classfile.constantpool.Utf8Entry;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

//spring-messaging e spring-websocket sono opzionali: solo le classi STOMP possono riferirli, altrimenti SSE fallirebbe senza
class ProtocolIsolationTest {

    private static final List<String> MESSAGING_PACKAGES = List.of("org/springframework/messaging/", "org/springframework/web/socket/");

    @Test
    void onlyStompClassesReferenceMessaging() throws IOException, URISyntaxException {
        final Path classes = Path.of(PushAutoConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Assertions.assertTrue(Files.isDirectory(classes), "Library classes not in a directory: " + classes);
        final List<Path> classFiles;
        try (Stream<Path> paths = Files.walk(classes)) {
            classFiles = paths.filter(path -> path.toString().endsWith(".class")).toList();
        }
        final List<String> violations = new ArrayList<>();
        final List<String> stompClassesWithMessaging = new ArrayList<>();
        for (Path classFile : classFiles) {
            final ClassModel model = ClassFile.of().parse(classFile);
            final String className = model.thisClass().asInternalName();
            if (!referencesMessaging(model)) continue;
            if (isStompClass(className)) {
                stompClassesWithMessaging.add(className);
            } else {
                violations.add(className);
            }
        }
        Assertions.assertEquals(List.of(), violations);
        //Controllo non vuoto: le classi STOMP riferiscono davvero il messaging
        Assertions.assertTrue(stompClassesWithMessaging.contains("com/fcusumano/push/controller/StompController"), stompClassesWithMessaging.toString());
    }

    //Nome dal file della classe: le annidate e quelle generate dal compilatore (es. StompController$1) seguono la classe esterna
    private static boolean isStompClass(String internalName) {
        final String outerName = internalName.substring(internalName.lastIndexOf('/') + 1).split("\\$")[0];
        return outerName.startsWith("Stomp") && !outerName.equals("StompImportSelector");
    }

    private static boolean referencesMessaging(ClassModel model) {
        for (PoolEntry entry : model.constantPool()) {
            if (entry instanceof Utf8Entry utf8 && MESSAGING_PACKAGES.stream().anyMatch(utf8.stringValue()::contains)) return true;
        }
        return false;
    }
}
