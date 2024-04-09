package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

public class FilterDataFromSql {
    public static void main(String args[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("C:\\Users\\91623\\Desktop\\springSecurity\\springSecurity\\src\\main\\resources\\exams\\students.csv");

        Dataset<Row> mathdata = dataset.filter("subject = 'Math'");

        Column col = dataset.col("subject");
        Column col1 = dataset.col("year");

        Dataset<Row> modernArtResult = dataset.filter(col.equalTo("Modern Art").and(col1.geq(2007)));

        modernArtResult.show();

//        mathdata.show();

        spark.close();
    }
}
