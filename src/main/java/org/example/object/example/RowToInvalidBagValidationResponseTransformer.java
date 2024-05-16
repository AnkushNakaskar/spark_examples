package org.example.object.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class RowToInvalidBagValidationResponseTransformer implements MapFunction<Row, ValidRowResponse>, Serializable {

    public static final String ROW = "row";
    public final String ID = "fileId";
    public final String KEY = "key";

    public static final String ERRORS = "errors";

    public static final String FIELD = "field";
    public static final String VALUE = "value";
    public static final String REASON = "reason";

    @Override
    public ValidRowResponse call(Row row) {
//        final Map<String, String> rowMap = this.getStringStringMap(row, ROW);
        ValidRowResponse validRowResponse =new ValidRowResponse();
        validRowResponse.setContent(row.getAs("content"));
        validRowResponse.setFileId(row.getAs(ID));
        validRowResponse.setKey(row.getAs(KEY));
        validRowResponse.setErrors( Arrays.stream((this.toArray(row.getAs(ERRORS))))
                .map(GenericRowWithSchema.class::cast)
                .map(this::convertToFailedValidationMessage)
                .collect(Collectors.toList()));
        return validRowResponse;
    }

    @NotNull
    private FailedValidationMessage convertToFailedValidationMessage(Object object) {
        Row row = (Row) object;
        FailedValidationMessage failedValidationMessage =new FailedValidationMessage();
        failedValidationMessage.setField(row.getAs(FIELD));
        failedValidationMessage.setReason(row.getAs(REASON));
        failedValidationMessage.setValue(row.getAs(VALUE));
        return failedValidationMessage;
    }

    public Map<String,String> getStringStringMap(final Row row, final String field) {
        return Objects.isNull(row.getAs(field)) ? new HashMap<>() :
                JavaConverters.mapAsJavaMapConverter(row.getAs(field)).asJava()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(entry -> String.valueOf(entry.getKey()),
                                entry -> String.valueOf(entry.getValue())));
    }

    public static Object[] toArray(final Object array) {
        return array instanceof WrappedArray
                ? (Object[])((WrappedArray) array).array()
                : (Object[]) array;
    }
}