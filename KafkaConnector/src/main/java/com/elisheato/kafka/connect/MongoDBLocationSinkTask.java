// MongoDBLocationSinkTask.java
package com.example.kafka.connect;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import org.json.JSONObject;

import java.util.Collection;
import java.util.Map;

public class MongoDBLocationSinkTask extends SinkTask {
    private MongoClient mongoClient;
    private String mongoUri;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        MongoDBLocationSinkConfig config = new MongoDBLocationSinkConfig(props);
        mongoUri = config.getString(MongoDBLocationSinkConfig.MONGO_URI_CONFIG);
        mongoClient = MongoClients.create(mongoUri);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        for (SinkRecord record : records) {
            String jsonStr = (String) record.value();
            
            try {
                JSONObject json = new JSONObject(jsonStr);
                String dbName = json.getString("dbName");
                
                // Get or create the database
                MongoDatabase database = mongoClient.getDatabase(dbName);
                
                // Get or create the collection
                MongoCollection<Document> collection = database.getCollection("locations");
                
                // Create document
                Document doc = new Document()
                        .append("userId", json.getString("userId"))
                        .append("deviceId", json.getString("deviceId"))
                        .append("latitude", json.getDouble("latitude"))
                        .append("longitude", json.getDouble("longitude"))
                        .append("accuracy", json.getDouble("accuracy"))
                        .append("timestamp", json.getLong("timestamp"));
                
                // Insert document
                collection.insertOne(doc);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}