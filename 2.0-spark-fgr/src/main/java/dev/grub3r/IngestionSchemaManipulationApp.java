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
    public void start(String source){
        System.out.println("*** SETTING UP SPARK SESSION ***");
        SparkSession spark = setupSparkSession();

        System.out.println("*** READING DATA ***");
        Dataset<Row> df = readFileSource(spark, source);

        df.show(5);

        System.out.println("*** TRANSFORMING DATASET ***");
        df = transformDataSet(df);

        df.show(5);

        System.out.println("*** ADDING CUSTOM ID ***");
        df = addID(df);

        df.show(5);
        df.printSchema();

        df = repartionDF(df, 4);

        spark.stop();
    }

    private SparkSession setupSparkSession(){
        SparkSession spark = SparkSession.builder()
                                    .appName("Load Restaurants")
                                    .master("local")
                                    .getOrCreate();
         return spark;
    }

    private Dataset<Row> readFileSource(SparkSession spark, String source)
    {
        Dataset<Row> df = spark.read().format("csv")
                    .option("header", "true")
                    .load(source);
        return df;
    }

    private Dataset<Row> transformDataSet(Dataset<Row> df)
    {
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
        return df;
    }

    private Dataset<Row> addID(Dataset<Row> df)
    {
        df = df.withColumn("id", functions.concat(
            df.col("state"),
            functions.lit("_"),
            df.col("county"), functions.lit("_"),
            df.col("datasetId")
        ));
        return df;
    }

    private Dataset<Row> repartionDF(Dataset<Row> df, int partCount)
    {
        // Partition[] partitions = df.rdd().partitions();
        // int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " + df.rdd().partitions().length);

        df = df.repartition(partCount);
        System.out.println("Partion count after repartition: " + df.rdd().partitions().length);
        return df;
    }
}