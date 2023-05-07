package com.ua.glebkorobov.fill;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class FillTableTest {


    @Test
    void testFastFill() throws InterruptedException {
        List<Document> documents = Arrays.asList(
                new Document("_id", "1"),
                new Document("_id", "2")
        );

        MongoCollection<Document> mockCollection = mock(MongoCollection.class);

        FillTable fillTable = new FillTable();

        fillTable.fastFill(mockCollection, documents);

        verify(mockCollection, times(1)).drop();
        verify(mockCollection).insertMany(anyList());
        verify(mockCollection, times(1)).countDocuments();
    }

    @Test
    void testCreateDocuments() {
        FillTable fillTable = new FillTable();

        List<Document> documents = fillTable.createDocumets(3);
        assertEquals(3, documents.size());
    }

}