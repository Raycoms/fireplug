package main.java.com.bag.main;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by daniel on 02/04/17.
 */
public class MultipleClientRunner {

    public class OutputPrinter extends Thread {
        private InputStream stream;
        private String clientName;

        public OutputPrinter(String clientName, InputStream stream) {
            this.stream = stream;
            this.clientName =clientName;
        }

        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(clientName + ": " + line);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public MultipleClientRunner() {

    }

    public void runClients(String option, int initialClient, int finalClient, int totalClients, String address) {
        try {
            Random rnd = new Random();
            System.out.printf("Starting...\n");
            List<Process> procs = new ArrayList<Process>();
            for (int i = initialClient; i <= finalClient; i++) {
                int serverPartner = i % 3;
                String cmd;
                if (option.equals("bag")) {
                    cmd = String.format("java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.main.RunTests true %d -1 1 %d %d false 2",
                            serverPartner, totalClients, i);
                }
                else if (option.equals("direct")) {
                    cmd = String.format("java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.main.RunTests false %s 8855 1 1 %d false 2",
                            address, i);
                }
                else {
                    System.out.println("Invalid option " + option);
                    return;
                }
                System.out.printf("Running Command: %s\n", cmd);
                /*ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-e", cmd);
                pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                Process proc = pb.start();*/
                Process proc = Runtime.getRuntime().exec(cmd);

                OutputPrinter printer = new MultipleClientRunner.OutputPrinter("Client " + i, proc.getInputStream());
                printer.start();
                procs.add(proc);
                Thread.sleep(rnd.nextInt(200));
            }

            for (Process p : procs)
                p.waitFor();

            System.out.printf("Finishing...\n");
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String opt = args[0];
        String address = null;
        int initialClient = Integer.parseInt(args[1]);
        int finalClient = Integer.parseInt(args[2]);
        int totalClients = Integer.parseInt(args[3]);
        if (args.length > 4)
            address = args[4];

        MultipleClientRunner runner = new MultipleClientRunner();
        runner.runClients(opt, initialClient, finalClient, totalClients, address);
    }
}
