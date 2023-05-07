package com.ua.glebkorobov;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ua.glebkorobov.fill.FillTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

public class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    private static final String URI = "mongodb+srv://gkorobov:Gfnhjy1801@cluster0.cnfozdn.mongodb.net/test";

    private static final String PRODUCT_TYPE = "type";

    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create(URI);
        MongoDatabase database = mongoClient.getDatabase("data");
        MongoCollection<Document> collection = database.getCollection("products");
        logger.info("Connection to db created");

        GetProperty getProperty = new GetProperty("myProp.properties");
        int countOfDocuments = Integer.parseInt(getProperty.getValueFromProperty("count_of_documents"));

        FillTable fillTable = new FillTable();

        fillTable.fastFill(collection, fillTable.createDocumets(countOfDocuments));

        FindAddress findAddress = new FindAddress();

        String productType = System.getProperty(PRODUCT_TYPE);
        logger.info("Get system property");

        if (productType == null) {
            logger.info("Got property was null. It will be set automatically Food");
            productType = "Food";
        }

        findAddress.findStoreWithMaxQuantityByType(collection, productType);

        mongoClient.close();
    }


}
