package com.equifax.api.core.gcp;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FirestoreConnection {
    private static final Logger logger = LoggerFactory.getLogger(FirestoreConnection.class);
    private Firestore db;

    public FirestoreConnection() {
        Firestore db = FirestoreOptions.getDefaultInstance().getService();
        this.db = db;
    }

    public FirestoreConnection(String projectId) throws Exception {
        // [START fs_initialize_project_id]
        FirestoreOptions firestoreOptions =
                FirestoreOptions.getDefaultInstance().toBuilder()
                        .setProjectId(projectId)
                        .setCredentials(GoogleCredentials.getApplicationDefault())
                        .build();
        Firestore db = firestoreOptions.getService();
        // [END fs_initialize_project_id]
        this.db = db;
    }

    Firestore getDb() {
        return db;
    }

    public void runAQuery() throws Exception {
        // [START fs_add_query]
        // asynchronously query for all users born before 1900
        List<String> name = new ArrayList<>();
        DocumentReference  docRef = db.collection("test_apps_reporting").document("EgxAlnX8TyiquDooU25q");
        ApiFuture<QuerySnapshot> query = db.collection("test_apps_reporting").get();
        QuerySnapshot querySnapshot = query.get();
        List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();



        for (QueryDocumentSnapshot document : documents) {
            System.out.println("clientAppDisplayName: " + document.getString("clientAppDisplayName"));
            System.out.println("name: " + document.getString("name"));
        }
        // [END fs_add_query]
    }

    public List<String> getName() throws Exception{
        List<String> name = new ArrayList<>();
        DocumentReference  docRef = db.collection("test_apps_reporting").document("EgxAlnX8TyiquDooU25q");
        ApiFuture<QuerySnapshot> query = db.collection("test_apps_reporting").get();
        QuerySnapshot querySnapshot = query.get();
        List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
        for (QueryDocumentSnapshot document : documents) {
           name.add(document.getString("name"));

        }
        return name;
    }

    void retrieveAllDocuments() throws Exception {
        // [START fs_get_all]
        // asynchronously retrieve all users
        ApiFuture<QuerySnapshot> query = db.collection("test_apps_reporting").get();
        // ...
        // query.get() blocks on response
        QuerySnapshot querySnapshot = query.get();
        List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
        for (QueryDocumentSnapshot document : documents) {
            logger.info("User: " + document.getId());
            System.out.println("First: " + document.getString("first"));
            if (document.contains("middle")) {
                System.out.println("Middle: " + document.getString("middle"));
            }
            System.out.println("Last: " + document.getString("last"));
            System.out.println("Born: " + document.getLong("born"));
        }
        // [END fs_get_all]
    }
}
