package at.ac.fhstp;

/**
 * Ingest the data!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Running Ingestion");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start(args[0]);
    }
}
