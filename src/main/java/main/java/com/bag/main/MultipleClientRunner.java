package main.java.com.bag.main;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Runs multiple clients at the same time.
 */
public class MultipleClientRunner
{

    public class OutputPrinter extends Thread
    {
        private InputStream stream;
        private String      clientName;

        public OutputPrinter(String clientName, InputStream stream)
        {
            this.stream = stream;
            this.clientName = clientName;
        }

        @Override
        public void run()
        {
            try
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String line;
                while ((line = reader.readLine()) != null)
                {
                    System.out.println(clientName + ": " + line);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * Standard constructor.
     */
    public MultipleClientRunner()
    {
        /*
         * Intentionally lef empty.
         */
    }

    /**
     * Run defined number of clients.
     * @param option (if bag or direct)
     * @param initialClient first client id
     * @param finalClient last client id
     * @param percOfWrites percentage of writes
     * @param amountsOfServers amount of servers to contact
     * @param addresses addresses of servers to contact.
     */
    private void runClients(final String option, final int initialClient, final int finalClient, final double percOfWrites, final int amountsOfServers, final String addresses)
    {
        try
        {
            Random rnd = new Random();
            System.out.printf("Starting...\n");
            List<Process> procs = new ArrayList<Process>();
            String[] directAddresses = new String[0];
            if (addresses != null)
            {
                directAddresses = addresses.split(",");
            }

            for (int i = initialClient; i <= finalClient; i++)
            {
                int serverPartner = i % amountsOfServers;
                String cmd;
                if (option.equals("bag"))
                {
                    cmd = String.format("java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.main.RunTests true %d -1 %d %s",
                            serverPartner, i, String.valueOf(percOfWrites).replace(',', '.'));
                }
                else if (option.equals("direct"))
                {
                    serverPartner = i % directAddresses.length;
                    String[] address = directAddresses[serverPartner].split(":");
                    cmd = String.format("java -cp build/libs/1.0-0.1-Setup-fat.jar main.java.com.bag.main.RunTests false %s %s %d %s",
                            address[0], address[1], i, String.valueOf(percOfWrites).replace(',', '.'));
                }
                else
                {
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
            {
                p.waitFor();
            }

            System.out.printf("Finishing...\n");
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        String opt = args[0];
        String address = null;
        int initialClient = Integer.parseInt(args[1]);
        int finalClient = Integer.parseInt(args[2]);
        double percOfWrites = Double.parseDouble(args[3]);
        int amountOfServers = Integer.parseInt(args[4]);
        if (args.length > 5)
        {
            address = args[5];
        }

        MultipleClientRunner runner = new MultipleClientRunner();
        runner.runClients(opt, initialClient, finalClient, percOfWrites, amountOfServers, address);
    }
}
