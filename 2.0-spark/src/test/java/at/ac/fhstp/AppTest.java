package at.ac.fhstp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    // Adopt to your local setup: /Users/martin/Software/FHSTP/repos
    String pathCSVFile = "/Users/martin/Software/FHSTP/repos/bdt-2021/data/input/spark/Restaurants_in_Wake_County.csv";

    SparkSession spark;
    Dataset<Row> df;

    @Before
    public void tearUp() {
        spark = SparkSession.builder()
        .appName("Restaurants in Wake County, NC")
        .master("local")
        .getOrCreate();
    }

    @After
    public void tearDown() {
        spark.stop();
    }

    @Test
    public void shouldSetupSparkSession() {
        assertThat(spark, is(notNullValue()));
    }

    @Test
    public void shouldReturnDataSet() {
        Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load(pathCSVFile);
        assertThat(df, is(notNullValue()));

        assertThat((int) df.count(), is(greaterThan(0)));
    }
}
