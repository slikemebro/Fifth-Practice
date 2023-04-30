package com.ua.glebkorobov;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;


public class FindAddress {

    private static final Logger logger = LogManager.getLogger(FindAddress.class);

    public Document findStoreWithMaxQuantityByType(MongoCollection<Document> collection, String type) {
        StopWatch watch = StopWatch.createStarted();
        logger.info("time started");

        AggregateIterable<Document> result = collection.aggregate(
                Arrays.asList(
                        Aggregates.match(Filters.eq("type", type)),
                        Aggregates.group("$address", Accumulators.sum("totalQuantity", "$quantity")),
                        Aggregates.sort(Sorts.descending("totalQuantity")),
                        Aggregates.limit(1)
                )
        );

        double time = watch.getTime(TimeUnit.MILLISECONDS) * 0.001;
        logger.info("find address time is = {} seconds", time);
        watch.stop();

        logger.info("address is {}", result.first().get("_id"));

        return result.first();
    }
}
