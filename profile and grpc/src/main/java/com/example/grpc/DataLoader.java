package com.example.grpc;

import io.grpc.*;

public class DataLoader
{
    public static void main( String[] args ) throws Exception
    {
        // Create a new server to listen on port 8080
        Server movieServer = ServerBuilder.forPort(8080)
                .addService(new MovieServiceImpl())
                .build();
        Server userServer = ServerBuilder.forPort(8081)
                .addService(new UserServiceImpl())
                .build();

        // Start the server
        movieServer.start();
        userServer.start();

        // Server threads are running in the background.
        System.out.println("MovieServer started");
        System.out.println("UserServer started");
        // Don't exit the main thread. Wait until server is terminated.
        movieServer.awaitTermination();
        userServer.awaitTermination();
    }
}
