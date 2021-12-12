
package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Properties;
//import static org.apache.spark.sql.functions.concat;
//import static org.apache.spark.sql.functions.lit;

/**
 * @author ankush.nakaskar
 * @version 1.0
 * @date 11/12/21 5:29 PM
 */
public class ConcatAndStoreInMysql {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Concat And Store In MysqL")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", "true")
                .load("data/authors.csv");
        df = df.withColumn(
                "name",
                functions.concat(df.col("lname"), functions.lit(", "), df.col("fname")));

        df.show();
        df.printSchema();
        String dbConnectionUrl = "jdbc:postgresql://localhost/test";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        df.write()
                .mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "ch02", prop);
    }
}


