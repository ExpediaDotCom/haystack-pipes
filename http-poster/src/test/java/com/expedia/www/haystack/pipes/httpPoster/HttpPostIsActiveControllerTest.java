package com.expedia.www.haystack.pipes.httpPoster;

import com.expedia.www.haystack.pipes.httpPoster.HttpPostIsActiveController.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;

import java.util.Set;

import static com.expedia.www.haystack.pipes.httpPoster.HttpPostIsActiveController.STARTUP_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpPostIsActiveControllerTest {
    private static final String[] ARGS = new String[0];

    @Mock
    private ProtobufToHttpPoster mockProtobufToHttpPoster;
    @Mock
    private Factory mockFactory;
    @Mock
    private SpringApplication mockSpringApplication;
    @Mock
    private Logger mockLogger;

    private Factory factory;

    @Before
    public void setUp() {
        storeHttpPostIsActiveControllerWithMocksInStaticInstance();
        factory = new Factory();
    }

    private void storeHttpPostIsActiveControllerWithMocksInStaticInstance() {
        new HttpPostIsActiveController(mockProtobufToHttpPoster, mockFactory, mockLogger);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(mockProtobufToHttpPoster, mockFactory, mockLogger);
        clearHttpPostIsActiveControllerInStaticInstance();
    }

    private void clearHttpPostIsActiveControllerInStaticInstance() {
        HttpPostIsActiveController.INSTANCE.set(null);
    }

    @Test
    public void testMainWithMockObjects() {
        when(mockFactory.createSpringApplication()).thenReturn(mockSpringApplication);

        HttpPostIsActiveController.main(ARGS);

        verify(mockLogger).info(STARTUP_MSG);
        verify(mockProtobufToHttpPoster).main();
        verify(mockFactory).createSpringApplication();
        verify(mockSpringApplication).run(ARGS);
    }

    @Test
    public void testFactoryCreateSpringApplication() {
        final SpringApplication springApplication = factory.createSpringApplication();

        final Set<Object> sources = springApplication.getSources();
        assertEquals(1, sources.size());
        final Object[] objects = sources.toArray();
        assertSame(HttpPostIsActiveController.class, objects[0]);
    }
}
