package com.ua.glebkorobov.fill;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FillTable {

    private static final Logger logger = LogManager.getLogger(FillTable.class);


    public void fillStores(MongoDatabase database, String fileName) {
        MyCSVReader reader = new MyCSVReader();
        MongoCollection<Document> collection = database.getCollection("stores");

        collection.drop();

        List<WriteModel<Document>> list = new ArrayList<>();
        List<String[]> stores = reader.readCSVFile(fileName);

        StopWatch watch = StopWatch.createStarted();

        for (String[] store : stores) {
            Document document = new Document("address", store[0]);
            list.add(new InsertOneModel<>(document));
        }
        collection.bulkWrite(list);

        double time = watch.getTime(TimeUnit.MILLISECONDS) * 0.001;
        logger.info("Fill product time is = {} seconds", time);
        logger.info("rps is {}", 9 / time);
        watch.stop();
    }

    public void fillProducts(MongoDatabase database, String fileName, int countOfProducts) {
        MyCSVReader reader = new MyCSVReader();
        MongoCollection<Document> collection = database.getCollection("products");

        collection.drop();

        List<WriteModel<Document>> list = new ArrayList<>(countOfProducts);

        int bufferSize = 150000;

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);

        List<String[]> types = reader.readCSVFile(fileName);

        AtomicInteger counter = new AtomicInteger(0);
        Random random = ThreadLocalRandom.current();

        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();

        List<Product> productsList =
                Stream.generate(() -> new Product(
                                RandomStringUtils.randomAlphabetic(5, 10),
                                types.get(random.nextInt(types.size()))[0]
                        ))
                        .peek(p -> {
                            if (validator.validate(p).isEmpty()) {
                                counter.addAndGet(1);
                            }
                        })
                        .takeWhile(p -> counter.intValue() <= countOfProducts)
                        .collect(Collectors.toList());

        StopWatch watch = StopWatch.createStarted();

        for (int i = 0; i < countOfProducts; i++) {
            Product product = productsList.get(i);
            Document documentOfProduct = new Document("name", product.getName()).append("type", product.getType());
            list.add(new InsertOneModel<>(documentOfProduct));
            if (list.size() >= bufferSize){
                collection.bulkWrite(list, bulkWriteOptions);
                list.clear();
            }
        }
        if (!list.isEmpty()){
            collection.bulkWrite(list, bulkWriteOptions);
        }

        double time = watch.getTime(TimeUnit.MILLISECONDS) * 0.001;
        logger.info("Fill product time is = {} seconds", time);
        logger.info("rps is {}", countOfProducts / time);
        watch.stop();
    }
}
