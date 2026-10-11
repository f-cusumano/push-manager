package com.fcusumano.push.util;

import com.fcusumano.push.TestPayloads.TestChannels;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class StompDestinationsTest {

    private static final StompDestinations DESTINATIONS = StompDestinations.of("/push");

    @Test
    void pushDestinationsAreThoseUnderThePrefix() {
        Assertions.assertTrue(DESTINATIONS.isPush("/push"));
        Assertions.assertTrue(DESTINATIONS.isPush("/push/USERS"));
        Assertions.assertTrue(DESTINATIONS.isPush("/push/filter/sub-0"));
        Assertions.assertFalse(DESTINATIONS.isPush("/pushX/USERS")); //Stesso inizio, altro segmento
        Assertions.assertFalse(DESTINATIONS.isPush(null));
        Assertions.assertFalse(DESTINATIONS.isPush("/topic/x"));
    }

    @Test
    void topicIsTheSingleSegmentAfterThePrefix() {
        Assertions.assertEquals("USERS", DESTINATIONS.topic("/push/USERS"));
        Assertions.assertNull(DESTINATIONS.topic("/push/"));
        Assertions.assertNull(DESTINATIONS.topic("/push"));
        Assertions.assertNull(DESTINATIONS.topic("/push/A/B"));
        Assertions.assertNull(DESTINATIONS.topic("/push/filter/x"));
        Assertions.assertNull(DESTINATIONS.topic("/topic/USERS"));
        Assertions.assertNull(DESTINATIONS.topic(null));
    }

    @Test
    void filterSubscriptionIdFollowsTheFilterSegment() {
        Assertions.assertEquals("sub-0", DESTINATIONS.filterSubscriptionId("/push/filter/sub-0"));
        Assertions.assertNull(DESTINATIONS.filterSubscriptionId("/push/filter/"));
        Assertions.assertNull(DESTINATIONS.filterSubscriptionId("/push/filter/ "));
        Assertions.assertNull(DESTINATIONS.filterSubscriptionId("/push/filter"));
        Assertions.assertNull(DESTINATIONS.filterSubscriptionId("/push/USERS"));
        Assertions.assertNull(DESTINATIONS.filterSubscriptionId(null));
    }

    @Test
    void destinationsAreBuiltFromThePrefix() {
        Assertions.assertEquals("/push/USERS", DESTINATIONS.channel(TestChannels.USERS));
        Assertions.assertEquals("/push/filter/sub-0", DESTINATIONS.filter("sub-0"));
        Assertions.assertEquals("USERS", DESTINATIONS.topic(DESTINATIONS.channel(TestChannels.USERS)));
        Assertions.assertEquals("sub-0", DESTINATIONS.filterSubscriptionId(DESTINATIONS.filter("sub-0")));
        //Prefisso su più segmenti
        final StompDestinations nested = StompDestinations.of("/app/push");
        Assertions.assertEquals("USERS", nested.topic(nested.channel(TestChannels.USERS)));
        Assertions.assertFalse(nested.isPush("/push/USERS"));
    }

    @Test
    void invalidPrefixesAreRejected() {
        for (String prefix : Arrays.asList("push", "/push/", "/", "", null)) {
            try {
                StompDestinations.of(prefix);
                Assertions.fail("Prefix '" + prefix + "' should be rejected");
            } catch (IllegalArgumentException e) {
                Assertions.assertTrue(e.getMessage().startsWith("push-manager.stomp.destination-prefix must start with '/'"), e.getMessage());
            }
        }
    }
}
