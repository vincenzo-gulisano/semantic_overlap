package spark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class StringServer {
    public static void main(String[] args) {

        Options options = new Options();
        options.addOption("i", "inputFile", true, "Input file path");
        options.addOption("ir", "injectionRateOutputFile", true, "Injection rate output file");
        options.addOption("d", "duration", true, "Duration in seconds");
        options.addOption("st", "sleepTime", true, "Sleep time in milliseconds");
        options.addOption("p", "port", true, "port");
        options.addOption("ip", "IP", true, "IP");
        options.addOption("wu", "warmup", true, "warmup");

        CommandLineParser parser = new GnuParser();

        try {
            CommandLine cmd = parser.parse(options, args);

            String inputFile = cmd.getOptionValue("i");
            int port = Integer.valueOf(cmd.getOptionValue("p"));
            String injectionRateOutputFile = cmd.getOptionValue("ir");
            long duration = Long.parseLong(cmd.getOptionValue("d"));
            long sleepTime = Long.parseLong(cmd.getOptionValue("st"));
            long warmUp = Long.parseLong(cmd.getOptionValue("wu"));

            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println("Server is listening on " + port);

                try (Socket clientSocket = serverSocket.accept()) {
                    System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());
                    OutputStream out = clientSocket.getOutputStream();
                    sendFileLines(inputFile, out, duration, sleepTime, injectionRateOutputFile, warmUp);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Closing server");
        } catch (ParseException e) {
            System.err.println("Error parsing command line options: " + e.getMessage());
            System.exit(1);
        }

    }

    private static void sendFileLines(String inputFilePath, OutputStream outputStream, long duration,
            long sleepPeriod, String statFile, long warmUp) {

        try {
            System.out.println("Source sleeping for " + warmUp + " ms to let Spark warm up");
            Thread.sleep(warmUp);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long startTime = System.currentTimeMillis();
        CountStat log = new CountStat(statFile, true);

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            while ((line = reader.readLine()) != null && System.currentTimeMillis() - startTime <= duration) {

                long beforeSend = System.nanoTime();
                String ts = System.currentTimeMillis() + "";
                outputStream.write((ts + '\t' + line).getBytes());
                outputStream.write('\n');
                outputStream.flush();
                log.increase(1);
                while (System.nanoTime() < beforeSend + sleepPeriod) {
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.close();
    }
}
