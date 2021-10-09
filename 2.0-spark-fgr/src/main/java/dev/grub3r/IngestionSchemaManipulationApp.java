package dev.grub3r;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.functions;

import java.util.Arrays;

public class IngestionSchemaManipulationApp {
    public static void start(String source){
        System.out.println("Hello from Ingestion");

        SparkSession spark = setupSparkSession();

        Dataset<Row> df = spark.read().format("csv")
            .option("header", "true")
            .load(source);

        df.show(5);

        df = df.withColumn("county", functions.lit("Wake"))
            .withColumnRenamed("HSISID", "datasetId")
            .withColumnRenamed("NAME", "name")
            .withColumnRenamed("ADDRESS1", "address1")
            .withColumnRenamed("ADDRESS2", "address2")
            .withColumnRenamed("CITY", "city")
            .withColumnRenamed("STATE", "state")
            .withColumnRenamed("POSTALCODE", "zip")
            .withColumnRenamed("PHONENUMBER", "tel")
            .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
            .withColumnRenamed("FACILITYTYPE", "type")
            .withColumnRenamed("X", "geoX")
            .withColumnRenamed("Y", "geoY")
            .drop("OBJECTID")
            .drop("PERMITID")
            .drop("GEOCODESTATUS");

        df.show(5);

        df = df.withColumn("id", functions.concat(
            df.col("state"),
            functions.lit("_"),
            df.col("county"), functions.lit("_"),
            df.col("datasetId")
        ));

        df.show(5);
    }

    public static SparkSession setupSparkSession(){
        SparkSession spark = SparkSession.builder()
                                    .appName("Load Restaurants")
                                    .master("local")
                                    .getOrCreate();
         return spark;
    }
}