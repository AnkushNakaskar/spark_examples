package org.example.object.example;


import java.io.Serializable;
import java.util.List;

public class ValidRowResponse implements Serializable {
    private  String fileId;
    private  String key;
    private  String content;
    private  String schemaName;

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getFetchedRowData() {
        return fetchedRowData;
    }

    public void setFetchedRowData(String fetchedRowData) {
        this.fetchedRowData = fetchedRowData;
    }

    public List<FailedValidationMessage> getErrors() {
        return errors;
    }

    public void setErrors(List<FailedValidationMessage> errors) {
        this.errors = errors;
    }

    private String fetchedRowData;
    private List<FailedValidationMessage> errors;

}
