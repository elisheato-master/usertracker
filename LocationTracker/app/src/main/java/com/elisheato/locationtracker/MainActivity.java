// MainActivity.java
package com.elisheato.locationtracker;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;

import android.Manifest;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.location.Location;
import android.os.Bundle;
import android.os.Looper;
import android.widget.TextView;
import android.widget.Toast;

import com.google.android.gms.location.FusedLocationProviderClient;
import com.google.android.gms.location.LocationCallback;
import com.google.android.gms.location.LocationRequest;
import com.google.android.gms.location.LocationResult;
import com.google.android.gms.location.LocationServices;
import com.google.android.gms.tasks.OnSuccessListener;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MainActivity extends AppCompatActivity {

    private static final int LOCATION_PERMISSION_REQUEST_CODE = 1001;
    private FusedLocationProviderClient fusedLocationClient;
    private LocationCallback locationCallback;
    private LocationRequest locationRequest;
    private TextView locationTextView;
    private KafkaProducer<String, String> producer;
    private String topicName = "user-loc-updates";
    private String userName;
    private String dbName;
    private String deviceId;
    private ScheduledExecutorService executorService;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        
        locationTextView = findViewById(R.id.locationTextView);
        
        // Get or generate user name from SharedPreferences
        SharedPreferences prefs = getSharedPreferences("UserPrefs", Context.MODE_PRIVATE);
        userName = prefs.getString("username", "DefaultUser");
        
        // Generate a unique device ID if not already saved
        deviceId = prefs.getString("device_id", UUID.randomUUID().toString());
        prefs.edit().putString("device_id", deviceId).apply();
        
        // Generate DB name based on user's name and current date
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyy");
        String currentDate = sdf.format(new Date());
        dbName = userName + currentDate;
        
        TextView dbNameTextView = findViewById(R.id.dbNameTextView);
        dbNameTextView.setText("Database: " + dbName);
        
        // Initialize location services
        fusedLocationClient = LocationServices.getFusedLocationProviderClient(this);
        createLocationRequest();
        createLocationCallback();
        
        // Initialize Kafka in a background thread
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.execute(this::initKafka);
        
        // Check and request permissions
        if (ActivityCompat.checkSelfPermission(this, 
                Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            requestLocationPermission();
        } else {
            startLocationUpdates();
        }
    }
    
    private void initKafka() {
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "YOUR_KAFKA_BOOTSTRAP_SERVERS");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "location-tracker-" + deviceId);
            
            // Security configuration for Confluent Cloud
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"YOUR_API_KEY\" password=\"YOUR_API_SECRET\";");
            
            producer = new KafkaProducer<>(props);
            
            runOnUiThread(() -> Toast.makeText(MainActivity.this, 
                    "Kafka connection established", Toast.LENGTH_SHORT).show());
        } catch (Exception e) {
            e.printStackTrace();
            runOnUiThread(() -> Toast.makeText(MainActivity.this, 
                    "Failed to connect to Kafka: " + e.getMessage(), Toast.LENGTH_LONG).show());
        }
    }
    
    private void sendLocationToKafka(Location location) {
        if (producer == null) return;
        
        executorService.execute(() -> {
            try {
                // Create JSON payload
                String timestamp = String.valueOf(System.currentTimeMillis());
                String payload = String.format(
                        "{\"userId\":\"%s\",\"deviceId\":\"%s\",\"dbName\":\"%s\",\"latitude\":%.6f,\"longitude\":%.6f,\"accuracy\":%.2f,\"timestamp\":%s}",
                        userName, deviceId, dbName, location.getLatitude(), location.getLongitude(), 
                        location.getAccuracy(), timestamp);
                
                ProducerRecord<String, String> record = 
                        new ProducerRecord<>(topicName, deviceId, payload);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    
    private void createLocationRequest() {
        locationRequest = LocationRequest.create()
                .setInterval(10000)        // 10 seconds
                .setFastestInterval(5000)  // 5 seconds
                .setPriority(LocationRequest.PRIORITY_HIGH_ACCURACY);
    }
    
    private void createLocationCallback() {
        locationCallback = new LocationCallback() {
            @Override
            public void onLocationResult(@NonNull LocationResult locationResult) {
                for (Location location : locationResult.getLocations()) {
                    updateLocationUI(location);
                    sendLocationToKafka(location);
                }
            }
        };
    }
    
    private void updateLocationUI(Location location) {
        String locationText = String.format("Lat: %.6f, Lng: %.6f\nAccuracy: %.2f meters", 
                location.getLatitude(), location.getLongitude(), location.getAccuracy());
        locationTextView.setText(locationText);
    }
    
    private void startLocationUpdates() {
        if (ActivityCompat.checkSelfPermission(this, 
                Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            return;
        }
        
        // Get last known location
        fusedLocationClient.getLastLocation().addOnSuccessListener(this, new OnSuccessListener<Location>() {
            @Override
            public void onSuccess(Location location) {
                if (location != null) {
                    updateLocationUI(location);
                    sendLocationToKafka(location);
                }
            }
        });
        
        // Start receiving location updates
        fusedLocationClient.requestLocationUpdates(locationRequest, locationCallback, Looper.getMainLooper());
    }
    
    private void requestLocationPermission() {
        ActivityCompat.requestPermissions(this,
                new String[]{Manifest.permission.ACCESS_FINE_LOCATION},
                LOCATION_PERMISSION_REQUEST_CODE);
    }
    
    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, 
                                           @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == LOCATION_PERMISSION_REQUEST_CODE) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                startLocationUpdates();
            } else {
                Toast.makeText(this, "Location permission denied", Toast.LENGTH_SHORT).show();
            }
        }
    }
    
    @Override
    protected void onResume() {
        super.onResume();
        if (ActivityCompat.checkSelfPermission(this, 
                Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) {
            startLocationUpdates();
        }
    }
    
    @Override
    protected void onPause() {
        super.onPause();
        fusedLocationClient.removeLocationUpdates(locationCallback);
    }
    
    @Override
    protected void onDestroy() {
        super.onDestroy();
        if (producer != null) {
            producer.close();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
    }
}