package dev.grub3r;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Hello World");

        IngestionSchemaManipulationApp ingestion = new IngestionSchemaManipulationApp();
        ingestion.start("../data/input/spark/Restaurants_in_Wake_County.csv");
    }
}
