package org.example.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @author ankush.nakaskar
 * @version 1.0
 * @date 26/01/22 12:57 PM
 */
public class FileStreaming {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        FileStreaming streaming =new FileStreaming();
        streaming.startStreamJob();
    }

    private void startStreamJob() throws StreamingQueryException, TimeoutException {
        String schemaString = "id,authorId,title,releaseDate,link";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);


        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample application file streaming")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("INFO");

        //build the streaming data reader from the file source, specifying csv file format

        Dataset<Row> rawData = spark.readStream().option("header", true).format("csv").schema(schema)
                .csv("/Users/ankush.nakaskar/Office/newCode/spark_examples/data/streaming/*.csv");
        rawData.createOrReplaceTempView("empData");
//count of employees grouping by department

        Dataset<Row> result = spark.sql("select count(*), authorId from  empData group by authorId");


        //write stream to output console with update mode as data is being aggregated

        StreamingQuery query = result.writeStream().outputMode(OutputMode.Update()).format("console").start();

        query.awaitTermination();
    }
}
