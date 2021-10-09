package dev.grub3r;

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
    String pathCSVFile = "../data/input/spark/Restaurants_in_Wake_County.csv";
    String pathJSONFile = "../data/input/spark/Restaurants_in_Durham_County_NC.json";

    IngestionSchemaManipulationApp cut;

    @Before
    public void tearUp() {
        cut = new IngestionSchemaManipulationApp();
    }

    @After
    public void tearDown(){
        cut.stopSpark();
    }

    @Test
    public void shouldReturnDataSet() {
        cut.start(pathCSVFile, pathJSONFile);
    }
}
