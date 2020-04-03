package com.rickie;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SecondaryIndexingApp
{
    public static void main( String[] args ){
        System.out.println( "Hello World!" );
        SpringApplication.run(SecondaryIndexingApp.class, args);
    }
}
