package com.ua.glebkorobov;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class FindAddressTest {


    @Test
    void testFindStoreWithMaxQuantityByType() {
        Document doc1 = new Document("_id", "Address 1").append("totalQuantity", 100);

        MongoCollection<Document> collection = mock(MongoCollection.class);

        AggregateIterable<Document> result = mock(AggregateIterable.class);

        when(result.first()).thenReturn(doc1);
        when(collection.aggregate(anyList())).thenReturn(result);

        FindAddress findAddress = new FindAddress();

        Document actual = findAddress.findStoreWithMaxQuantityByType(collection, "Test Type");

        verify(collection, times(1)).aggregate(anyList());
        assertEquals(doc1, actual);
        assertEquals("Address 1", actual.get("_id"));
    }

    @Test
    void testFindStoreWithMaxQuantityByTypeWithError() {
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(collection.aggregate(Mockito.anyList())).thenThrow(new RuntimeException());
        FindAddress findAddress = new FindAddress();

        assertThrows(RuntimeException.class,
                () -> findAddress.findStoreWithMaxQuantityByType(collection, "Test Type"));
    }

    @Test
    void testFindStoreWithMaxQuantityByTypeWithEmptyCollection() {
        AggregateIterable<Document> result = mock(AggregateIterable.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);

        FindAddress findAddress = new FindAddress();

        when(result.first()).thenReturn(null);
        when(collection.aggregate(Mockito.anyList())).thenReturn(result);

        assertThrows(NullPointerException.class,
                () -> findAddress.findStoreWithMaxQuantityByType(collection, "Test Type"));
    }

    @Test
    void testFindStoreWithMaxQuantityByTypeWithNullType() {
        AggregateIterable<Document> result = mock(AggregateIterable.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);

        when(result.first()).thenReturn(null);
        when(collection.aggregate(Mockito.anyList())).thenReturn(result);

        FindAddress findAddress = new FindAddress();

        assertThrows(NullPointerException.class,
                () -> findAddress.findStoreWithMaxQuantityByType(collection, null));
    }
}