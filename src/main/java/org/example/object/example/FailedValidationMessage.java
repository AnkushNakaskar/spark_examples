package org.example.object.example;

import java.io.Serializable;

public class FailedValidationMessage implements Serializable {
    private  String field;
    private  String value;
    private  String reason;

    @Override
    public String toString() {
        return "FailedValidationMessage{" +
                "field='" + field + '\'' +
                ", value='" + value + '\'' +
                ", reason='" + reason + '\'' +
                '}';
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}
