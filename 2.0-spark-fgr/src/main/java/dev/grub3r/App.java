package dev.grub3r;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Running IngestionSchemaManipulation");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        IngestionSchemaManipulationApp ingestion = new IngestionSchemaManipulationApp();
        ingestion.start(args[0]);
    }
}
