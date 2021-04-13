import com.pvub.coinmixer.CoinMixer;
import com.pvub.coinmixer.User;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestMixer {
    protected final Logger logger      = LoggerFactory.getLogger("MXR");

    public TestMixer() {

    }

    @Test
    public void testTransfer() {
        // The config to initialize the rest of the system
        Path configJson     = Paths.get("src/test/resources/config.json");
        try {
            // Read the config to initialize
            String config_content = readFileContent(configJson);
            JsonObject appConfig = new JsonObject(config_content);
            CoinMixer mixer = new CoinMixer(appConfig, Schedulers.io());

            // add users to mixer
            User user = null;
            JsonArray userArray = appConfig.getJsonArray("users");
            if (userArray.size() > 0) {
                JsonObject userObj = userArray.getJsonObject(0);
                user = new User(userObj);
                mixer.addUser(user);
            }
            ArrayList<String> destinationAddresses = user.getDestinationAddress();

            // Get initial amount at User's destination addresses
            double userAmountBefore = 0.0d;
            for (String destinationAddress : destinationAddresses) {
                userAmountBefore += mixer.balance(destinationAddress);
            }

            // start the mixer
            mixer.start();

            // Make a test transfer to User's deposit address
            double testTransferAmount = 10.0d;
            assert(mixer.transfer("testInitiateAddress", user.getDepositAddress(), testTransferAmount));

            CountDownLatch latch = new CountDownLatch(1);
            final double beforeTestBalance = userAmountBefore;
            Observable.interval(2, TimeUnit.SECONDS)
                    .subscribe(t -> {
                                double totalBalance = 0.0d;
                                for (String destinationAddress : destinationAddresses) {
                                    totalBalance += mixer.balance(destinationAddress);
                                }
                                logger.info("Balance {}", totalBalance);
                                if (totalBalance == (testTransferAmount + beforeTestBalance)) {
                                    logger.info("Transfer verified");
                                    latch.countDown();
                                }
                            },
                            error -> {
                                logger.error("Error fetching balance", error);
                                assert(false);
                            },
                            () -> {});

            assert(latch.await(30, TimeUnit.SECONDS));
            mixer.stop();
        } catch (Exception e) {
            assert(false);
        }
    }

    private String readFileContent(Path fileToRead) throws IOException {
        String fileContent = "";
        BufferedReader reader = null;
        try
        {
            reader = new BufferedReader(new FileReader(fileToRead.toFile()));

            //Reading all the lines of template text file into templateContent
            String line = reader.readLine();
            while (line != null)
            {
                fileContent = fileContent + line + System.lineSeparator();
                line = reader.readLine();
            }
        }
        catch (IOException e)
        {
            logger.error("Error reading file {}", fileToRead, e);
            throw e;
        }
        finally
        {
            try
            {
                // Closing the resources
                if (reader != null) {
                    reader.close();
                }
            }
            catch (IOException e)
            {
                logger.error("Error closing file {}", fileToRead, e);
                throw e;
            }
        }
        return fileContent;
    }
}
