/*
 * Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.jsonTransformer;

import com.expedia.www.haystack.pipes.jsonTransformer.JsonTransformerIsActiveController.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.SpringApplication;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JsonTransformerIsActiveControllerTest {
    @Mock
    private Factory mockFactory;
    private Factory realFactory;

    @Mock
    private ProtobufToJsonTransformer mockProtobufToJsonTransformer;

    @Mock
    private SpringApplication mockSpringApplication;

    @Before
    public void setUp() {
        realFactory = JsonTransformerIsActiveController.factory;
        JsonTransformerIsActiveController.factory = mockFactory;
    }

    @After
    public void tearDown() {
        JsonTransformerIsActiveController.factory = realFactory;
        verifyNoMoreInteractions(mockFactory, mockProtobufToJsonTransformer, mockSpringApplication);
    }

    @Test
    public void testMain() {
        when(mockFactory.createProtobufToJsonTransformer()).thenReturn(mockProtobufToJsonTransformer);
        when(mockFactory.createSpringApplication()).thenReturn(mockSpringApplication);

        final String[] args = new String[0];
        JsonTransformerIsActiveController.main(args);

        verify(mockFactory).createProtobufToJsonTransformer();
        verify(mockFactory).createSpringApplication();
        verify(mockProtobufToJsonTransformer).main();
        verify(mockSpringApplication).run(args);
    }

    @Test
    public void testDefaultConstructor() {
        new JsonTransformerIsActiveController();
    }

    @Test
    public void testFactoryCreateProtobufToJsonTransformer() {
        realFactory.createProtobufToJsonTransformer();
    }

    @Test
    public void testFactoryCreateSpringApplication() {
        final SpringApplication springApplication = realFactory.createSpringApplication();

        final Set<Object> sources = springApplication.getSources();
        assertEquals(Collections.singleton(JsonTransformerIsActiveController.class), sources);
    }
}
