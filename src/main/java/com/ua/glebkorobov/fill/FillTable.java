package com.ua.glebkorobov.fill;

import com.mongodb.client.MongoCollection;
import com.ua.glebkorobov.MyCSVReader;
import com.ua.glebkorobov.dto.Product;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class FillTable {

    private static final Logger logger = LogManager.getLogger(FillTable.class);
    public static final int BACH_SIZE = 10000;
    public static final int COUNT_OF_THREAD_POLL = 30;


    /**
     * mongodb id without index
     * 2023-04-30 18:34:03 INFO  time started
     * 2023-04-30 18:37:22 INFO  Fill product time is = 199.15 seconds
     * 2023-04-30 18:37:22 INFO  rps is 15064.022093899071
     * <p>
     * mongodb id with index
     * 2023-04-30 18:39:10 INFO  time started
     * 2023-04-30 18:43:01 INFO  Fill product time is = 231.303 seconds
     * 2023-04-30 18:43:01 INFO  rps is 12970.000389100012
     * <p>
     * custom id without index
     * 2023-04-30 18:44:26 INFO  time started
     * 2023-04-30 18:48:31 INFO  Fill product time is = 245.191 seconds
     * 2023-04-30 18:48:31 INFO  rps is 12235.359372897048
     */
    public void fastFill(MongoCollection<Document> collection, List<Document> documents) {
        collection.drop();

        StopWatch watch = StopWatch.createStarted();
        logger.info("time started");

        ExecutorService executorService = Executors.newFixedThreadPool(COUNT_OF_THREAD_POLL);

        for (int i = 0; i < documents.size(); i += BACH_SIZE) {
            int endIndex = Math.min(i + BACH_SIZE, documents.size());
            List<Document> batch = documents.subList(i, endIndex);
            executorService.submit(() -> collection.insertMany(batch));
        }


        executorService.shutdown();
        while (!executorService.isTerminated()) {
        }

        double time = watch.getTime(TimeUnit.MILLISECONDS) * 0.001;
        logger.info("Fill product time is = {} seconds", time);
        logger.info("rps is {}", documents.size() / time);
        watch.stop();

        logger.info("Count in table {}", collection.countDocuments());
    }


    public List<Document> createDocumets(int countOfDocuments) {
        MyCSVReader reader = new MyCSVReader();
        List<String[]> types = reader.readCSVFile("tables/types.csv");
        List<String[]> stores = reader.readCSVFile("tables/stores.csv");

        Random random = ThreadLocalRandom.current();

        int typeSize = types.size();
        int storeSize = stores.size();

        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();

        int inValidCounter = 0;

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < countOfDocuments; ) {
            Product product = new Product(
                    RandomStringUtils.randomAlphabetic(5, 16),
                    types.get(random.nextInt(typeSize))[0],
                    random.nextInt(300),
                    stores.get(random.nextInt(storeSize))[0]
            );

//            if (validator.validate(product).isEmpty()){
                Document document = new Document("name", product.getName())
                        .append("type", product.getType())
                        .append("address", product.getAddress())
                        .append("quantity", product.getQuantity());
                documents.add(document);
                i++;
//            }else {
//                inValidCounter++;
//            }
        }

//        logger.info("Invalid porducts {}", inValidCounter);
        return documents;
    }
}
