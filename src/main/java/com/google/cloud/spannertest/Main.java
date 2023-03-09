package com.google.cloud.spannertest;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import com.google.spanner.admin.database.v1.DatabaseDialect;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {
  
  public static void main(String[] args) throws Exception {
    try (DatabaseAdminClient client =
        DatabaseAdminClient.create(
            DatabaseAdminSettings.newBuilder()
                .setEndpoint("preprod-spanner.sandbox.googleapis.com:443")
                .setCredentialsProvider(
                    FixedCredentialsProvider.create(
                        GoogleCredentials.fromStream(
                            Files.newInputStream(
                                Paths.get("/home/loite/Downloads/span-cloud-testing.json")))))
                .build())) {
      client.createDatabaseAsync(CreateDatabaseRequest.newBuilder()
          .setCreateStatement("create database \"knut-test-db3\"")
          .setParent("projects/span-cloud-testing/instances/spanner-testing")
          .setDatabaseDialect(DatabaseDialect.POSTGRESQL)
          .build()).get();
      System.out.println("Created database");
      System.out.println(client.getDatabaseDdl("projects/span-cloud-testing/instances/spanner-testing/databases/knut-test-db3"));

      System.out.println(
          client
              .updateDatabaseDdlAsync(
                  UpdateDatabaseDdlRequest.newBuilder()
                      .setDatabase(
                          "projects/span-cloud-testing/instances/spanner-testing/databases/knut-test-db3")
                      .addStatements("create table test (id bigint primary key)")
                      .build())
              .get());
      System.out.println("Created table test");

      // This statement will work, as the table definition is the same.
      System.out.println(
          client
              .updateDatabaseDdlAsync(
                  UpdateDatabaseDdlRequest.newBuilder()
                      .setDatabase(
                          "projects/span-cloud-testing/instances/spanner-testing/databases/knut-test-db3")
                      .addStatements("create table if not exists test (id bigint primary key)")
                      .build())
              .get());
      System.out.println("Tried to create table test once more");

      // This statement seems to fail, because the table definition is different.
      System.out.println(
          client
              .updateDatabaseDdlAsync(
                  UpdateDatabaseDdlRequest.newBuilder()
                      .setDatabase(
                          "projects/span-cloud-testing/instances/spanner-testing/databases/knut-test-db3")
                      .addStatements("create table if not exists test (id bigint primary key, value varchar)")
                      .build())
              .get());
      System.out.println("Tried to create a different table test");
    }
  }

}
