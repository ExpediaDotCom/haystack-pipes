package com.expedia.www.haystack.pipes.secretDetector;

import org.junit.Before;
import org.junit.Test;

import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.RANDOM;
import static com.expedia.www.haystack.pipes.commons.test.TestConstantsAndCommonCode.SERVICE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

public class FinderNameAndServiceNameTest {
    private static final String FINDER_NAME = RANDOM.nextLong() + "FINDER_NAME";

    private FinderNameAndServiceName finderNameAndServiceName;

    @Before
    public void setUp() {
        finderNameAndServiceName = new FinderNameAndServiceName(FINDER_NAME, SERVICE_NAME);
    }

    @Test
    public void testGetFinderName() {
        assertEquals(FINDER_NAME, finderNameAndServiceName.getFinderName());
    }

    @Test
    public void testGetServiceName() {
        assertEquals(SERVICE_NAME, finderNameAndServiceName.getServiceName());
    }

    @Test
    public void testEqualsNullOther() {
        //noinspection SimplifiableJUnitAssertion,ConstantConditions,ObjectEqualsNull
        assertFalse(finderNameAndServiceName.equals(null));
    }

    @Test
    public void testEqualsSameOther() {
        assertEquals(finderNameAndServiceName, finderNameAndServiceName);
    }

    @Test
    public void testEqualsDifferentClassOther() {
        //noinspection SimplifiableJUnitAssertion,EqualsBetweenInconvertibleTypes
        assertFalse(finderNameAndServiceName.equals(FINDER_NAME));
    }

    @Test
    public void testEqualsTotalMatch() {
        final FinderNameAndServiceName finderNameAndServiceName
                = new FinderNameAndServiceName(FINDER_NAME, SERVICE_NAME);
        assertEquals(finderNameAndServiceName, finderNameAndServiceName);
    }

    @Test
    public void testEqualsFinderNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        final FinderNameAndServiceName finderNameAndServiceName23
                = new FinderNameAndServiceName("2", "3");
        assertNotEquals(finderNameAndServiceName13, finderNameAndServiceName23);
    }

    @Test
    public void testEqualsServiceNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName12
                = new FinderNameAndServiceName("1", "2");
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        assertNotEquals(finderNameAndServiceName12, finderNameAndServiceName13);
    }

    @Test
    public void testHashCodeFinderNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        final FinderNameAndServiceName finderNameAndServiceName23
                = new FinderNameAndServiceName("2", "3");
        assertNotEquals(finderNameAndServiceName13.hashCode(), finderNameAndServiceName23.hashCode());
    }

    @Test
    public void testHashCodeServiceNameMisMatch() {
        final FinderNameAndServiceName finderNameAndServiceName12
                = new FinderNameAndServiceName("1", "2");
        final FinderNameAndServiceName finderNameAndServiceName13
                = new FinderNameAndServiceName("1", "3");
        assertNotEquals(finderNameAndServiceName12.hashCode(), finderNameAndServiceName13.hashCode());
    }

}
