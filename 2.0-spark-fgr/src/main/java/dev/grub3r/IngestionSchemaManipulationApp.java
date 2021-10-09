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
    private SparkSession _spark;

    public IngestionSchemaManipulationApp(){
        startSpark();
    }

    public void startSpark()
    {
        System.out.println("*** SETTING UP SPARK SESSION ***");
        _spark = setupSparkSession();
    }

    public void stopSpark()
    {
        System.out.println("*** STOPPING SPARK SESSION ***");
        _spark.stop();
    }

    public void start(String sourceCSV, String sourceJSON){
        Dataset<Row> dfCSV = perfomCSVIngestion(sourceCSV);
        Dataset<Row> dfJSON = performJSONIngestion(sourceJSON);
    }

    private Dataset<Row> perfomCSVIngestion(String source)
    {
        System.out.println("*** READING DATA CSV ***");
        Dataset<Row> df = readFileSourceCSV(source);

        df.show(5);

        System.out.println("*** TRANSFORMING DATASET ***");
        df = transformDataSetCSV(df);

        df.show(5);

        System.out.println("*** ADDING CUSTOM ID ***");
        df = addID(df);

        df.show(5);
        df.printSchema();

        df = repartionDF(df, 4);

        return df;
    }

    private Dataset<Row> performJSONIngestion(String source)
    {
        System.out.println("*** READING DATA JSON ***");
        Dataset<Row> df = readFileSourceJSON(source);
        df.show(5);
        df = df.withColumn("datasedId", df.col("fields.id"));
        df.show(5);

        df = transformDataSetJSON(df);
        df.show(5);
        return df;
    }

    private SparkSession setupSparkSession(){
        SparkSession spark = SparkSession.builder()
                                    .appName("Load Restaurants")
                                    .master("local")
                                    .getOrCreate();
         return spark;
    }

    private Dataset<Row> readFileSourceCSV(String source)
    {
        Dataset<Row> df = _spark.read().format("csv")
                    .option("header", "true")
                    .load(source);
        return df;
    }

    private Dataset<Row> readFileSourceJSON(String source)
    {
        Dataset<Row> df = _spark.read().format("json")
                    .load(source);
        return df;
    }

    private Dataset<Row> transformDataSetCSV(Dataset<Row> df)
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

    private Dataset<Row> transformDataSetJSON(Dataset<Row> df)
    {
        df = df.withColumn("county", functions.lit("Durham"))
                .withColumn("datasedId", df.col("fields.id"))
                .withColumn("name",df.col("fields.premise_name"))
                .withColumn("address1",df.col("fields.premise_address1"))
                .withColumn("address2",df.col("fields.premise_address2"))
                .withColumn("city",df.col("fields.premise_city"))
                .withColumn("state",df.col("fields.premise_state"))
                .withColumn("zip",df.col("fields.premise_zip"))
                .withColumn("phone",df.col("fields.premise_phone"))
                .withColumn("dateStart",df.col("fields.opening_date"))
                .withColumn("dateEnd",df.col("fields.closing_date"))
                .withColumn("type",df.col("fields.type_description"));
                // .withColumn("geoX", df.col("fields.geolocation").toArray[0]);

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
        System.out.println("Partition count before repartition: " + df.rdd().partitions().length);

        df = df.repartition(partCount);
        System.out.println("Partion count after repartition: " + df.rdd().partitions().length);
        return df;
    }
}