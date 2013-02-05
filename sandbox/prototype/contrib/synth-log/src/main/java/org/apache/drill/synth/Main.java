package org.apache.drill.synth;


import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

/**
 * Create a query log with a specified number of log lines and an associated user profile database.
 * <p/>
 * Command line args include number of log lines to generate, the name of the log file to generate and the
 * name of the file to store the user profile database in.
 * <p/>
 * Log lines and user profile entries are single line JSON.
 */
public class Main {
    public static void main(String[] args) throws IOException {

        int n = Integer.parseInt(args[0].replaceAll("[KMG]?$", ""));

        switch (args[0].charAt(args[0].length() - 1)) {
            case 'G':
                n *= 1e9;
                break;
            case 'M':
                n *= 1e6;
                break;
            case 'K':
                n *= 1e3;
                break;
            default:
                // no suffix leads here
                break;
        }

        LogGenerator lg = new LogGenerator();
        BufferedWriter log = Files.newWriter(new File(args[1]), Charsets.UTF_8);
        for (int i = 0; i < n; i++) {
            if (i % 10000 == 0) {
                System.out.printf("%d %d\n", i, lg.getUserCount());
            }
            log.write(lg.sample().toString());
            log.newLine();
        }
        log.close();

        BufferedWriter profile = Files.newWriter(new File(args[2]), Charsets.UTF_8);
        for (User user : lg.getUsers()) {
            profile.write(user.toString());
            profile.newLine();
        }
        profile.close();
    }
}
