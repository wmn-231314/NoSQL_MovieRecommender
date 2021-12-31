package com.example.grpc;

import com.service.RedisUtil;
import io.grpc.stub.StreamObserver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class UserServiceImpl extends UserServiceGrpc.UserServiceImplBase {
    @Override
    public void getUserHistory(UserRequest request, StreamObserver<UserHistoryResponse> responseObserver) {
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        jedis.select(1);
        String userId = "user_"+request.getUserId();
        Set<Tuple> history = jedis.zrangeWithScores(userId,0,-1);
        List<Rate> user_history = new ArrayList<>();
        Iterator it = history.iterator();
        while(it.hasNext()){
            Tuple rating = (Tuple) it.next();
            String info[] = rating.getElement().split("_");
            Rate rate = Rate.newBuilder()
                    .setMovieId(Integer.parseInt(info[0]))
                    .setScore(Float.parseFloat(info[1]))
                    .setTimestamp((long) rating.getScore())
                    .build();
            user_history.add(rate);
        }
        UserHistoryResponse response = UserHistoryResponse.newBuilder()
                .addAllRate(user_history)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
