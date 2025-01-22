package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author ankush.nakaskar
 */
public class SchemaExercise {

    public static void main(String[] args) throws JsonProcessingException {

        SparkSession spark = SparkSession.builder()
                .appName("Ankush Sample application")
                .master("local")
                .getOrCreate();

        //Check the person.json file and contains of list which belong to person class, also check for array of integers

        StructType companySchema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("marks",DataTypes.createArrayType(DataTypes.StringType,false))
                ;

        String jsonString =getJsonString();
       System.out.println(jsonString);
//       Exception in thread "main" org.apache.spark.sql.AnalysisException: Path does not exist: file:/Users/ankush.nakaskar/Office/newCode/spark_examples/person.json
        Dataset<Row> df = spark.read().schema(companySchema).json("./data/person.json").toDF();
        df.show(5);
//        TypeTags.TypeTagImpl<Person> value12 = new TypeTags.TypeTagImpl<Person>();

        Encoder<Person> employeeEncoder = Encoders.bean(Person.class);
        Dataset<Person> ds = spark.read().json("./data/person.json").as(employeeEncoder);

        ds.show(5);



    }

    private static String getJsonString() throws JsonProcessingException {
        ObjectMapper mapper =new ObjectMapper();
        List<Person> personList =new ArrayList<>();
        personList.add(new Person("Ankush",21, Arrays.asList(1,2,3)));
        personList.add(new Person("Ankush1",22, Arrays.asList(1,2,3)));
        personList.add(new Person("Ankush2",23, Arrays.asList(1,2,3)));
        personList.add(new Person("Ankush3",24, Arrays.asList(1,2,3)));
        personList.add(new Person("Ankush4",25, Arrays.asList(1,2,3)));
        String result = mapper.writeValueAsString(personList);
        return result;
    }

}
