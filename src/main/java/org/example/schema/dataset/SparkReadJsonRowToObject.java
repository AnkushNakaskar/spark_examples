package org.example.schema.dataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.expr;

/**
 * @author ankush.nakaskar
 */
public class SparkReadJsonRowToObject {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample Json read in Row application")
                .master("local")
                .getOrCreate();

        Dataset<Employee> dataframe = spark.read().json("./data/employee_object_dataset.json").map(new RowObjectToEmployeeMapper(), Encoders.bean(Employee.class));
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

    static class RowObjectToEmployeeMapper implements MapFunction<Row, Employee>, Serializable {

        @Override
        public Employee call(Row row) {
            Employee employee =new Employee();
            employee.setName(row.getAs("name"));
            Long value = row.getAs("age");
            employee.setAge(value.intValue());
            return employee;
        }


    }

}
