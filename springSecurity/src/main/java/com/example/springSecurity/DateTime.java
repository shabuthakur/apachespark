package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DateTime {
    public static void main(String args[]) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        List<Row> inMemory = new ArrayList<Row>();

        inMemory.add(RowFactory.create("WARN","2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL","2015-12-31 03:19:32"));
        inMemory.add(RowFactory.create("WARN","2016-12-31 04:18:32"));
        inMemory.add(RowFactory.create("FATAL","2016-12-31 05:19:32"));
        inMemory.add(RowFactory.create("WARN","2013-01-31 04:19:22"));
        inMemory.add(RowFactory.create("WARN","2015-05-31 11:19:32"));
        inMemory.add(RowFactory.create("INFO","2022-03-31 09:19:32"));

        StructField[] structFields = new StructField[]{
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("datetime",DataTypes.StringType,false,Metadata.empty())
        };

        StructType schema = new StructType(structFields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory,schema);


        //GROUP BY DATA

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> result = spark.sql("select level,date_format(datetime,'MMM') as month from logging_table");

        result.createOrReplaceTempView("logging_table");

        result.show();




        spark.close();
    }
}
