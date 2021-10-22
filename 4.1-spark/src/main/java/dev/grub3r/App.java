package dev.grub3r;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        System.out.println("Running DartPiApproxy");
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        DartPiApproxy ingestion = new DartPiApproxy();
        ingestion.start(1);
        // ingestion.stopSpark();
        // Test Comment
    }
}
