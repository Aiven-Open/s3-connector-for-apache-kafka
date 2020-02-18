/*
 * Copyright (C) 2020 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.s3.templating;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TemplatingEngineTest {
    @Test
    public void testEmptyString() {
        final TemplatingEngine te = new TemplatingEngine();
        assertEquals("", te.render(""));
    }

    @Test
    public void testNoVariables() {
        final TemplatingEngine te = new TemplatingEngine();
        assertEquals("somestring", te.render("somestring"));
    }

    @Test
    public void testNewLine() {
        final TemplatingEngine te = new TemplatingEngine();
        assertEquals("some\nstring", te.render("some\nstring"));
    }

    @Test
    public void testVariableFormatNoSpaces() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo", te.render("{{foo}}"));
    }

    @Test
    public void testVariableFormatLeftSpace() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo", te.render("{{ foo}}"));
    }

    @Test
    public void testVariableFormatRightSpace() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo", te.render("{{foo }}"));
    }

    @Test
    public void testVariableFormatBothSpaces() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo", te.render("{{ foo }}"));
    }

    @Test
    public void testVariableFormatMultipleSpaces() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo", te.render("{{   foo  }}"));
    }

    @Test
    public void testVariableFormatTabs() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo", te.render("{{\tfoo\t}}"));
    }

    @Test
    public void testVariableInBeginning() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("foo END", te.render("{{ foo }} END"));
    }

    @Test
    public void testVariableInMiddle() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("BEGINNING foo END", te.render("BEGINNING {{ foo }} END"));
    }

    @Test
    public void testVariableInEnd() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        assertEquals("BEGINNING foo", te.render("BEGINNING {{ foo }}"));
    }

    @Test
    public void testNonBoundVariable() {
        final TemplatingEngine te = new TemplatingEngine();
        assertEquals("BEGINNING {{ foo }}", te.render("BEGINNING {{ foo }}"));
    }

    @Test
    public void testMultipleVariables() {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        te.bindVariable("bar", () -> "bar");
        te.bindVariable("baz", () -> "baz");
        assertEquals("1foo2bar3baz4", te.render("1{{foo}}2{{bar}}3{{baz}}4"));
    }

    @Test
    public void testBigListOfNaughtyStringsJustString() throws IOException {
        final TemplatingEngine te = new TemplatingEngine();
        for (final String line : getBigListOfNaughtyStrings()) {
            assertEquals(line, te.render(line));
        }
    }

    @Test
    public void testBigListOfNaughtyStringsWithVariableInBeginning() throws IOException, URISyntaxException {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        for (final String line : getBigListOfNaughtyStrings()) {
            assertEquals("foo" + line, te.render("{{ foo }}" + line));
        }
    }

    @Test
    public void testBigListOfNaughtyStringsWithVariableInEnd() throws IOException {
        final TemplatingEngine te = new TemplatingEngine();
        te.bindVariable("foo", () -> "foo");
        for (final String line : getBigListOfNaughtyStrings()) {
            assertEquals(line + "foo", te.render(line + "{{ foo }}"));
        }
    }

    private Collection<String> getBigListOfNaughtyStrings() throws IOException {
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("blns.txt");
             final InputStreamReader reader = new InputStreamReader(is);
             final BufferedReader bufferedReader = new BufferedReader(reader)) {

            return bufferedReader.lines().filter(s -> !s.isEmpty() && !s.startsWith("#"))
                    .collect(Collectors.toList());
        }
    }
}
