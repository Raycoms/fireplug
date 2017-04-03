package main.java.com.bag.main;

import jdk.nashorn.internal.ir.RuntimeNode;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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

    public void runClients(int initialClient, int finalClient, int totalClients) {
        try {
            System.out.printf("Starting...\n");
            List<Process> procs = new ArrayList<Process>();
            for (int i = initialClient; i <= finalClient; i++) {
                int serverPartner = i % 3;
                String cmd = String.format("java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.main.RunTests true %d -1 1 %d %d false 2",
                        serverPartner, totalClients, i);
                System.out.printf("Running Command: %s\n", cmd);
                /*ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-e", cmd);
                pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                Process proc = pb.start();*/
                Process proc = Runtime.getRuntime().exec(cmd);

                OutputPrinter printer = new MultipleClientRunner.OutputPrinter("Client " + i, proc.getInputStream());
                printer.start();
                procs.add(proc);
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
        int initialClient = Integer.parseInt(args[0]);
        int finalClient = Integer.parseInt(args[1]);
        int totalClients = Integer.parseInt(args[2]);

        MultipleClientRunner runner = new MultipleClientRunner();
        runner.runClients(initialClient, finalClient, totalClients);
    }
}
