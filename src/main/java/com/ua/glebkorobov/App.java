package com.ua.glebkorobov;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.ua.glebkorobov.fill.FillTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Hello world!
 */
public class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    private static final String URI = "mongodb+srv://gkorobov:Gfnhjy1801@cluster0.cnfozdn.mongodb.net/test";

    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create(URI);
        MongoDatabase database = mongoClient.getDatabase("data");

        FillTable fillTable = new FillTable();

        fillTable.fillStores(database, "tables/stores.csv");

        fillTable.fillProducts(database, "tables/types.csv", 334000);


    }


}
