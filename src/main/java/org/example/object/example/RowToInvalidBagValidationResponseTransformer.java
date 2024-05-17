package org.example.object.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class RowToInvalidBagValidationResponseTransformer implements MapFunction<Row, InvalidFileBag>, Serializable {

    public static final String ROW = "row";
    public final String ID = "fileId";
    public final String KEY = "key";

    public static final String ERRORS = "errors";

    public static final String FIELD = "field";
    public static final String VALUE = "value";
    public static final String REASON = "reason";

    @Override
    public InvalidFileBag call(Row row) {
//        final Map<String, String> rowMap = this.getStringStringMap(row, ROW);
        InvalidFileBag invalidFileBag =new InvalidFileBag();
        invalidFileBag.setFileId(row.getAs(ID));
        invalidFileBag.setTotalRows(row.getAs("totalRows"));
        Map<Object, Object> currentBuffer = JavaConverters.mapAsJavaMapConverter(row.getAs("errors")).asJava();
        Map<String, List<FailedValidationMessage>> mapError =new HashMap<>();
        invalidFileBag.setErrors(mapError);
        currentBuffer.forEach((key,value) ->{
            String mapKey = String.valueOf(key);
            List<FailedValidationMessage> valuesList1 = Arrays.stream((RowToInvalidBagValidationResponseTransformer.toArray(value)))
                    .map(GenericRowWithSchema.class::cast)
                    .map(this::convertToFailedValidationMessage)
                    .collect(Collectors.toList());
           mapError.put(mapKey,valuesList1);

        });

//        invalidFileBag.setErrors(currentBuffer);
        return invalidFileBag;
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