package com.ua.glebkorobov.fill;

import com.mongodb.client.MongoCollection;
import com.ua.glebkorobov.MyCSVReader;
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
    public static final int INSERT_PACK = 100000;
    public static final int BACH_SIZE = 10000;
    public static final int COUNT_OF_THREAD_POLL = 30;


    /**
     * mongodb id without index
     * 2023-04-30 18:34:03 INFO  time started
     * 2023-04-30 18:37:22 INFO  Fill product time is = 199.15 seconds
     * 2023-04-30 18:37:22 INFO  rps is 15064.022093899071
     *
     * mongodb id with index
     * 2023-04-30 18:39:10 INFO  time started
     * 2023-04-30 18:43:01 INFO  Fill product time is = 231.303 seconds
     * 2023-04-30 18:43:01 INFO  rps is 12970.000389100012
     *
     * custom id without index
     * 2023-04-30 18:44:26 INFO  time started
     * 2023-04-30 18:48:31 INFO  Fill product time is = 245.191 seconds
     * 2023-04-30 18:48:31 INFO  rps is 12235.359372897048
     * @param collection
     * @param documents
     */
    public void fastFill(MongoCollection<Document> collection, List<Document> documents) {
        collection.drop();

//        collection.createIndex(Indexes.ascending("type", "address"));



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



    public List<Document> createDocumets(int countOfDocuments){
        MyCSVReader reader = new MyCSVReader();
        List<String[]> types = reader.readCSVFile("tables/types.csv");
        List<String[]> stores = reader.readCSVFile("tables/stores.csv");

        Random random = ThreadLocalRandom.current();

        int typeSize = types.size();
        int storeSize = stores.size();

        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < countOfDocuments; i++) {
            Document document = new Document("name", RandomStringUtils.randomAlphabetic(2, 15))
                    .append("type", types.get(random.nextInt(typeSize))[0])
                    .append("address", stores.get(random.nextInt(storeSize))[0])
                    .append("quantity", random.nextInt(300));
            documents.add(document);
        }
        return documents;
    }
}
