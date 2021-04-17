import com.pvub.parser.LogParser;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ParserTest {
    public ParserTest() {}

    @Test
    public void testParser() {
        LogParser parser = new LogParser();
        parser.parse("application.log", "errors.json");

        Path path = Paths.get("errors.json");
        try {
            FileReader reader = new FileReader(path.toFile());
            BufferedReader br=new BufferedReader(reader);
            StringBuffer sb=new StringBuffer();
            String line;
            while((line=br.readLine())!=null)
            {
                sb.append(line);
            }
            reader.close();    //closes the stream and release the resources
            JsonObject obj = new JsonObject(sb.toString());
            long errorCount = obj.getLong("errorCount");
            assert(errorCount == 4);
        } catch (IOException e) {
            assert(false);
        }
    }
}
