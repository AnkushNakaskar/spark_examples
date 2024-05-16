package org.example.object.example;


import java.io.Serializable;
import java.util.List;

public class FileIdInvalidRowCountMapResponse implements Serializable {
    private  String fileId;

    private Long count;

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "FileIdInvalidRowCountMapResponse{" +
                "fileId='" + fileId + '\'' +
                ", count=" + count +
                '}';
    }
}
