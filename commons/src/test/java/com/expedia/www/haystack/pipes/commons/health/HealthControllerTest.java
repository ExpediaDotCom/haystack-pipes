package com.expedia.www.haystack.pipes.commons.health;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.HEALTHY;
import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.NOT_SET;
import static com.expedia.www.haystack.pipes.commons.health.HealthController.HealthStatus.UNHEALTHY;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class HealthControllerTest {

    @Mock
    private HealthStatusListener mockHealthStatusListener;

    private HealthController healthController;

    @Before
    public void setUp() {
        healthController = new HealthController();
        healthController.addListener(mockHealthStatusListener);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockHealthStatusListener);
    }

    @Test
    public void testConstructor() {
        assertEquals(NOT_SET, healthController.getStatus());
    }

    @Test
    public void testSetHealthyWasNotSet() {
        healthController.setHealthy();

        assertEquals(HEALTHY, healthController.getStatus());
        verify(mockHealthStatusListener).onChange(HEALTHY);
    }

    @Test
    public void testSetHealthyWasHealthy() {
        healthController.setHealthy();
        healthController.setHealthy();

        assertEquals(HEALTHY, healthController.getStatus());
        verify(mockHealthStatusListener).onChange(HEALTHY);
    }

    @Test
    public void testSetUnhealthyWasNotSet() {
        healthController.setUnhealthy();

        assertEquals(UNHEALTHY, healthController.getStatus());
        verify(mockHealthStatusListener).onChange(UNHEALTHY);
    }

    @Test
    public void testSetUnhealthyWasNotHealthy() {
        healthController.setUnhealthy();
        healthController.setUnhealthy();

        assertEquals(UNHEALTHY, healthController.getStatus());
        verify(mockHealthStatusListener).onChange(UNHEALTHY);
    }

}
