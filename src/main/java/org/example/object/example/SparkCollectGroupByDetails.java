package org.example.object.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
    This program explains the accumulator in spark
    1. Accumulator is use when you have some counter and you can refer that in driver node.
        -> Like , in below example , we want to know , how many even number are there.
        -> My laptop has 10 partition because , there are 10 available processor.
        -> 1-100 : data is divided into 10 partition, hence every partition will have 5 even number.
        -> You can refer this : http://localhost:4040/stages/stage/?id=0&attempt=0
        -> at the end you can get the value 50 in total.
        https://stackoverflow.com/questions/69295448/aggregate-dataset-by-key-and-list-the-rest-of-values
 */








public class SparkCollectGroupByDetails {

    public static void main(String[] args) {
        SparkCollectGroupByDetails df =new SparkCollectGroupByDetails();
        df.start();
    }
    public void start() {
        int numberOfThrows = 100;
        LongAccumulator accEven = new LongAccumulator();
        CollectionAccumulator<String> stringCollectionAccumulator =new CollectionAccumulator<>();
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Pi")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().register(accEven,"evenNumber");
        spark.sparkContext().register(stringCollectionAccumulator,"evenNumberList");
        ValidRowResponse validRowResponse1 =new ValidRowResponse();
        validRowResponse1.setContent("validRowResponse1_Content");
        validRowResponse1.setFileId("validRowResponse1_FileId");
        validRowResponse1.setKey("validRowResponse1_Key");
        validRowResponse1.setSchemaName("validRowResponse1_SchemaName");
        List<FailedValidationMessage> validRowResponse1Errors =new ArrayList<>();
        FailedValidationMessage validRowResponse1Failed = new FailedValidationMessage();
        validRowResponse1Failed.setField("validRowResponse1Failed_field");
        validRowResponse1Failed.setReason("validRowResponse1Failed_reason");
        validRowResponse1Failed.setValue("validRowResponse1Failed_Value");
        validRowResponse1Errors.add(validRowResponse1Failed);
        validRowResponse1.setErrors(validRowResponse1Errors);




        ValidRowResponse validRowResponse2 =new ValidRowResponse();
        validRowResponse2.setContent("validRowResponse2_Content");
        validRowResponse2.setFileId("validRowResponse2_FileId");
        validRowResponse2.setKey("validRowResponse2_Key");
        validRowResponse2.setSchemaName("validRowResponse2_SchemaName");
        List<FailedValidationMessage> validRowResponse2Errors =new ArrayList<>();
        FailedValidationMessage validRowResponse2Failed = new FailedValidationMessage();
        validRowResponse2Failed.setField("validRowResponse2Failed_field");
        validRowResponse2Failed.setReason("validRowResponse2Failed_reason");
        validRowResponse2Failed.setValue("validRowResponse2Failed_Value");
        validRowResponse2Errors.add(validRowResponse2Failed);
        validRowResponse2.setErrors(validRowResponse2Errors);


        ValidRowResponse validRowResponse3 =new ValidRowResponse();
        validRowResponse3.setContent("validRowResponse3_Content");
        validRowResponse3.setFileId("validRowResponse2_FileId");
        validRowResponse3.setKey("validRowResponse3_Key");
        validRowResponse3.setSchemaName("validRowResponse3_SchemaName");
        List<FailedValidationMessage> validRowResponse3Errors =new ArrayList<>();
        FailedValidationMessage validRowResponse3Failed = new FailedValidationMessage();
        validRowResponse3Failed.setField("validRowResponse3Failed_field");
        validRowResponse3Failed.setReason("validRowResponse3Failed_reason");
        validRowResponse3Failed.setValue("validRowResponse3Failed_Value");
        validRowResponse3Errors.add(validRowResponse3Failed);
        validRowResponse3.setErrors(validRowResponse3Errors);


        List<ValidRowResponse> list = new ArrayList<>(numberOfThrows);
        list.add(validRowResponse1);
        list.add(validRowResponse2);
        list.add(validRowResponse3);
        Dataset<Row> incrementalDf = spark
                .createDataset(list, Encoders.bean(ValidRowResponse.class))
                .toDF();

        incrementalDf.show(10);
        incrementalDf.printSchema();

//        incrementalDf.filter((FilterFunction<ValidRowResponse>) response -> !response.isValid())
//                .map((MapFunction<ValidRowResponse, String>) ValidRowResponse::getFileId, Encoders.STRING())
//                .distinct()
//                .collectAsList();

        List<ValidRowResponse> listInvalidFileIds = incrementalDf.map(new RowToValidationResponseTransformer(), Encoders.bean(ValidRowResponse.class))
                .filter((FilterFunction<ValidRowResponse>) response -> !response.isValid())
                .collectAsList();
        System.out.println("!!!!!!...Ankush...!!!!!!");

        System.out.println(listInvalidFileIds);


//        List<String> values = incrementalDf.map(new RowToValidationResponseTransformer(), Encoders.bean(ValidRowResponse.class))
//                .filter((FilterFunction<ValidRowResponse>) response -> !response.isValid())
//                .map((MapFunction<ValidRowResponse, String>) validRowResponse -> {
//                    return validRowResponse.getFileId();
//                },Encoders.STRING())
////                .map((MapFunction<ValidRowResponse, String>) ValidRowResponse::getFileId, Encoders.STRING())
//                .collectAsList();

        Dataset<InvalidFileBag> mappingDF = incrementalDf.map(new RowToValidationResponseTransformer(), Encoders.bean(ValidRowResponse.class))
                .filter((FilterFunction<ValidRowResponse>) response -> !response.isValid())
                .map((MapFunction<ValidRowResponse, InvalidFileBag>) validRowResponse -> {
                    InvalidFileBag invalidFileBag = new InvalidFileBag();
                    invalidFileBag.setFileId(validRowResponse.getFileId());
                    invalidFileBag.setTotalRows(1L);
                    if (invalidFileBag.getErrors() == null)
                        invalidFileBag.setErrors(new HashMap<>());
                    invalidFileBag.getErrors().put(validRowResponse.getKey(), validRowResponse.getErrors());
                    return invalidFileBag;
                }, Encoders.bean(InvalidFileBag.class));

//                .map((MapFunction<ValidRowResponse, String>) ValidRowResponse::getFileId, Encoders.STRING())

        mappingDF.show(3);

        System.out.println("Partitions structure1111 : "+mappingDF.toJavaRDD().getNumPartitions());
        List<List<InvalidFileBag>> rows = mappingDF.toJavaRDD().glom().collect();
        System.out.println("Partitions structure1111 : "+rows);

        for( List<InvalidFileBag> row : rows){
            System.out.println("Partition Values length111 : "+row);
        }
        Dataset<InvalidFileBag> valuesUpdated = mappingDF.repartition(new Column("fileId"));
        valuesUpdated.show(3);
        System.out.println("Partitions structure2222 : "+valuesUpdated.toJavaRDD().getNumPartitions());
        List<List<InvalidFileBag>> rows1 = valuesUpdated.toJavaRDD().glom().collect();
        System.out.println("Partitions structure2222 : "+rows);

        for( List<InvalidFileBag> row : rows1){
            System.out.println("Partition Values length222 : "+row);
        }
        List<InvalidFileBag> values = valuesUpdated.collectAsList();
        valuesUpdated.groupBy("fileId").count().printSchema();


        System.out.println("!!!...Ankush...!!!");
        System.out.println(values);

        System.out.println("!!!...Ankush...!!!");
        List<FileIdInvalidRowCountMapResponse> countList = valuesUpdated.groupBy("fileId").count()
                .map(new RowToMapOfCountResponseTransformer(), Encoders.bean(FileIdInvalidRowCountMapResponse.class))
                .collectAsList();
        System.out.println(countList);
        Map<String, List<FileIdInvalidRowCountMapResponse>> mapOfFileIDCount = countList.stream().collect(Collectors.groupingBy(FileIdInvalidRowCountMapResponse::getFileId));
        System.out.println(mapOfFileIDCount);
//        valuesUpdated.groupBy("fileId").df().reduce()

        System.out.println("!!!.......Ankush.........!!!");
        valuesUpdated.groupBy("fileId").df().printSchema();

        List<InvalidFileBag> collectListValue = valuesUpdated.groupBy("fileId").df()
                .map(new RowToInvalidBagValidationResponseTransformer(), Encoders.bean(InvalidFileBag.class)).collectAsList();
        System.out.println("!!!.......Ankush.........!!!");
        System.out.println(collectListValue);
//        valuesUpdated.groupBy("fileId")


        valuesUpdated.groupBy("fileId").df()
                .map(new RowToInvalidBagValidationResponseTransformer(), Encoders.bean(InvalidFileBag.class));
        InvalidFileBag reducesResult = valuesUpdated.groupBy("fileId").df()
                .map(new RowToInvalidBagValidationResponseTransformer(), Encoders.bean(InvalidFileBag.class))

                .reduce((ReduceFunction<InvalidFileBag>) (invalidFileBag, t1) -> {
                    System.out.println("Values are :: "+ invalidFileBag);
                    InvalidFileBag target = new InvalidFileBag();
                    target.setFileId(invalidFileBag.getFileId());
                    target.setTotalRows(invalidFileBag.getTotalRows() + 1);
                    Map<String, List<FailedValidationMessage>> mapError = new HashMap<>(invalidFileBag.getErrors());
                    mapError.putAll(t1.getErrors());
                    target.setErrors(mapError);
                    return target;
                });


//        invalidFileBagDf.printSchema();
//        invalidFileBagDf.show();
        System.out.println("!!!.......Ankush.........!!!");
        System.out.println(reducesResult);
        System.out.println("!!!.......Ankush.........!!!");


    }
}
