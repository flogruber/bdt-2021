package dev.grub3r;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Running TransformationAndAction");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        TransformationAndAction ingestion = new TransformationAndAction();
        ingestion.start(args[0],"transform");
        // ingestion.stopSpark();
    }
}
