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

public class UserClient {
    private static final Logger logger = Logger.getLogger(UserClient.class.getName());

    private final ManagedChannel channel;
    private final UserServiceGrpc.UserServiceBlockingStub blockingStub;

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public UserClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    /** Construct client for accessing HelloWorld server using the existing channel. */
    UserClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = UserServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void getUserHistory(Long id) {
        String session = "user_"+id;
        logger.info("Will try to get history " + id + " ...");
        UserRequest request = UserRequest.newBuilder()
                .setSessionId(session)
                .setUserId(id).build();
        Iterator<UserHistoryResponse> response;
        try {
            response = blockingStub.getUserHistory(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        List<Rate> userrating = response.next().getRateList();
        Iterator<Rate> it = userrating.iterator();
        if(userrating.isEmpty()){
            System.out.println("user not exist");
        }else{
            while(it.hasNext()){
                Rate rating = it.next();
                System.out.println(rating.getMovieId()+" "+ rating.getScore()+" "+rating.getTimestamp());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        UserClient userClient = new UserClient("localhost", 8081);
        try {
            Scanner sc = new Scanner(System.in);
            Long userId = sc.nextLong();
            userClient.getUserHistory(userId);
        } finally {
            userClient.shutdown();
        }
    }
}
