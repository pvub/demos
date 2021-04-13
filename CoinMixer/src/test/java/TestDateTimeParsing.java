import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TestDateTimeParsing {
    @Test
    public void testParse() {
        String dtStr = "2021-04-12T22:45:30.268Z";
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        LocalDateTime localDateTime = LocalDateTime.parse(dtStr.replaceAll("Z$", "+0000"), dateFormatter);
        long ts = localDateTime.atZone(ZoneId.of("America/Chicago")).toInstant().toEpochMilli();
    }
}
