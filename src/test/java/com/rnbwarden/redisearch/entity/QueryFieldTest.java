package com.rnbwarden.redisearch.entity;

import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class QueryFieldTest {

    @Test
    public void testEscapeChars() {

        Stream.of(
                ",",
                ".",
                "<",
                ">",
                "{",
                "}",
                "[",
                "]",
                "\"",
                "'",
                ":",
                ";",
                "!",
                "@",
                "#",
                "$",
                "%",
                "^",
                "&",
                "*",
                "(",
                ")",
                "-",
                "+",
                "=",
                "~",
                "\\",
                " "
        ).forEach(c -> {
            assertEquals("field not properly escaped: '" + c + "'", ("\\" + c), QueryField.escapeSpecialCharacters(c));
        });
    }
}