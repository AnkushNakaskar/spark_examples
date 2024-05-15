package org.example.object.example;



import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class InvalidFileBag implements Serializable {
    private  String fileId;
    private Long totalRows;

    public Long getTotalRows() {
        return totalRows;
    }

    public void setTotalRows(Long totalRows) {
        this.totalRows = totalRows;
    }

    public Long getLastReadRow() {
        return lastReadRow;
    }

    public void setLastReadRow(Long lastReadRow) {
        this.lastReadRow = lastReadRow;
    }

    public Map<String, List<FailedValidationMessage>> getErrors() {
        return errors;
    }

    public void setErrors(Map<String, List<FailedValidationMessage>> errors) {
        this.errors = errors;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    private Long lastReadRow;
    private Map<String, List<FailedValidationMessage>> errors;

}
