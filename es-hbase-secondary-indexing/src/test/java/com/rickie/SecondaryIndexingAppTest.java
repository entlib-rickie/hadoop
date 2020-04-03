package com.rickie;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Unit test for simple SecondaryIndexingApp.
 */
public class SecondaryIndexingAppTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void dateTest() {
        String s = "2020-1-10";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = format.parse(s);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
        }
        System.out.println(format.format(date));
    }
}
