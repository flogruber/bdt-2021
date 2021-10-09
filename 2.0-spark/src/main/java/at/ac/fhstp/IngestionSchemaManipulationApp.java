package at.ac.fhstp;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IngestionSchemaManipulationApp {

    public void start(String source) {
        // Ingest the CSV
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load(source);
    }
}