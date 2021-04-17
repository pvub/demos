import com.pvub.parser.LogParser;
import org.junit.Test;

public class ParserTest {
    public ParserTest() {}

    @Test
    public void testParser() {
        LogParser parser = new LogParser();
        parser.parse("application.log", "errors.json");
    }
}
