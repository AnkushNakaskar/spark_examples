package org.example.schema.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.expr;

/**
 * @author ankush.nakaskar
 */
public class SparkColumnExpression {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample Expression and column names and modifications read in Row application")
                .master("local")
                .getOrCreate();

        Dataset<Employee> dataframe = spark.read().json("./data/employee_object_dataset.json").map(new SparkReadJsonRowToObject.RowObjectToEmployeeMapper(), Encoders.bean(Employee.class));
        dataframe.show(5);
        String[] columns = dataframe.columns();
        List<String> list = Arrays.asList(columns);
        System.out.println(list);
        Dataset<Row> result = dataframe.select(expr("age * 2"));
        result.show(5);
        result = dataframe.withColumnRenamed("age","multiAge");
        result.show(5);
        List<Employee> countList = dataframe
                .collectAsList();
        System.out.println(countList);

    }

}
