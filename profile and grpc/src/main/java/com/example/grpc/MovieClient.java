package com.example.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MovieClient {
    private static final Logger logger = Logger.getLogger(MovieClient.class.getName());

    private final ManagedChannel channel;
    private final MovieServiceGrpc.MovieServiceBlockingStub blockingStub;

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public MovieClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    /** Construct client for accessing HelloWorld server using the existing channel. */
    MovieClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = MovieServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getMovieProfile(Long id) {
        String session = "movie_"+id;
        logger.info("Will try to get history " + id + " ...");
        MovieRequest request = MovieRequest.newBuilder()
                .setSessionId(session)
                .setMovieId(id).build();
        Iterator<MovieProfileResponse> response;
        try {
            response = blockingStub.getMovieProfile(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
    }

    public static void main(String[] args) throws Exception {
        MovieClient movieClient = new MovieClient("localhost", 8081);
        try {
            Scanner sc = new Scanner(System.in);
            Long movId = sc.nextLong();
            movieClient.getMovieProfile(movId);
        } finally {
            movieClient.shutdown();
        }
    }
}
