package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sqlTables {
    public static void main(String args[]) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("C:\\Users\\91623\\Desktop\\springSecurity\\springSecurity\\src\\main\\resources\\exams\\students.csv");

       dataset.createOrReplaceTempView("my_student_table");

       Dataset<Row> results = spark.sql("select subject, year from my_student_table where subject = 'French'");
       results.show();

       //max score and same for avvg min etc
        Dataset<Row> maxScore = spark.sql("select max(score) from my_student_table where subject = 'French'");
        maxScore.show();

        //find diffrent years with order values

        Dataset<Row> years = spark.sql("select distinct(year) from my_student_table order by year");
        years.show();

        spark.close();
    }
}
