package org.example.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * @author ankush.nakaskar
 * @version 1.0
 * @date 26/01/22 6:10 PM
 */
public class RetailStreamExample {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        RetailStreamExample retailStreamExample =new RetailStreamExample();
        retailStreamExample.retailProcessing();
    }

    public void retailProcessing() throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample application file streaming")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("INFO");

        Dataset<Purchase> rawData1 = spark.read().option("header", true).format("csv")
                .csv("/Users/ankush.nakaskar/Office/newCode/spark_examples/data/retails/2010-12-01.csv").as(Encoders.bean(Purchase.class));
        StructType schema = rawData1.schema();

        Dataset<Purchase> rawData = spark.readStream().option("header", true).format("csv").schema(schema)
                .csv("/Users/ankush.nakaskar/Office/newCode/spark_examples/data/retails/*.csv").as(Encoders.bean(Purchase.class));

        Dataset<Row> rawData2 = rawData.selectExpr("CustomerId",
                "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy("CustomerId").sum("total_cost");

//        rawData2.show(50);
//        rawData.writeStream().format("console").queryName("customer_purchases_2").outputMode("complete").start();
        StreamingQuery query = rawData2.writeStream().outputMode(OutputMode.Update()).format("console").queryName("customer_purchases_2").start();

        query.awaitTermination();

    }

    static class Purchase implements Serializable {
//        InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country

        private Integer InvoiceNo;
        private String StockCode;
        private String Description;
        private Integer Quantity;
        private Date InvoiceDate;
        private Double UnitPrice;
        private Double CustomerID;
        private String Country;

    }

}

