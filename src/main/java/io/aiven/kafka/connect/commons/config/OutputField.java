package io.aiven.kafka.connect.commons.config;

import com.google.common.base.Objects;

public class OutputField {
    private final OutputFieldType fieldType;
    private final OutputFieldEncodingType encodingType;

    public OutputField(final OutputFieldType fieldType, final OutputFieldEncodingType encodingType) {
        this.fieldType = fieldType;
        this.encodingType = encodingType;
    }

    public OutputFieldType getFieldType() {
        return fieldType;
    }

    public OutputFieldEncodingType getEncodingType() {
        return encodingType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldType, encodingType);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof OutputField)) {
            return false;
        }

        final OutputField that = (OutputField) obj;

        return Objects.equal(this.fieldType, that.fieldType)
                && Objects.equal(this.encodingType, that.encodingType);
    }
}
