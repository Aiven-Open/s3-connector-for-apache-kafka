package io.aiven.kafka.connect.commons.templating;

public class TextTemplatePart implements TemplatePart {

    private final String text;

    protected TextTemplatePart(final String text) {
        this.text = text;
    }

    public final String text() {
        return text;
    }

}
