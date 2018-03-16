package com.expedia.www.haystack.pipes.commons.health;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.HEALTHY;
import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.UNHEALTHY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UpdateHealthStatusFileTest {

    private Path tempFile;

    private UpdateHealthStatusFile updateHealthStatusFile;

    @Before
    public void setUp() throws IOException {
        tempFile = Files.createTempFile("tempfiles", ".tmp");
        updateHealthStatusFile = new UpdateHealthStatusFile(tempFile.toString());
    }

    @After
    public void tearDown() throws IOException {
        Files.delete(tempFile);
    }

    @Test
    public void testOnChangeHealthy() throws IOException {
        updateHealthStatusFile.onChange(HEALTHY);

        verifyFileContents(true);
    }

    @Test
    public void testOnChangeUnhealthy() throws IOException {
        updateHealthStatusFile.onChange(UNHEALTHY);

        verifyFileContents(false);
    }

    private void verifyFileContents(boolean expected) throws IOException {
        final byte[] bytesFromFile = Files.readAllBytes(tempFile);
        final byte[] bytesFromExpected = Boolean.toString(expected).getBytes(UTF_8);
        assertArrayEquals(bytesFromExpected, bytesFromFile);
    }

    @Test
    public void testOnChangeException() {
        assertTrue(tempFile.toFile().setReadOnly());

        try {
            updateHealthStatusFile.onChange(HEALTHY);
        } catch(RuntimeException e) {
            assertEquals(AccessDeniedException.class, e.getCause().getClass());
        }
    }
}
