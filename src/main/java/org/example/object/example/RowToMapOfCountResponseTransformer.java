package org.example.object.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class RowToMapOfCountResponseTransformer implements MapFunction<Row, FileIdInvalidRowCountMapResponse>, Serializable {


    @Override
    public FileIdInvalidRowCountMapResponse call(Row row) {
        FileIdInvalidRowCountMapResponse validRowResponse =new FileIdInvalidRowCountMapResponse();
        validRowResponse.setCount(row.getAs("count"));
        validRowResponse.setFileId(row.getAs("fileId"));
        return validRowResponse;
    }


}