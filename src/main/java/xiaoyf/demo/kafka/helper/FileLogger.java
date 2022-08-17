package xiaoyf.demo.kafka.helper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;

public class FileLogger {
    static Path logfile = Path.of("consumer-seen-messages.log");
    public static void log(String s) throws Exception {
        File file = logfile.toFile();
        FileWriter fw = new FileWriter("consumer-seen-messages.log", true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(s);
        bw.newLine();
        bw.close();
    }

    public static String bytesString(byte[] bytes) {
        if (bytes == null || bytes.length ==0) {
            return "EMPTY[]";
        }

        StringBuilder b = new StringBuilder();
        b.append("[");
        for (byte by : bytes) {
            b.append(Byte.valueOf(by).toString()).append(",");
        }
        b.append("]");

        return b.toString();
    }
}
