package com.example.grpc;
import com.service.MovieInfo;
import com.service.RedisUtil;
import com.service.SerializeUtil;
import com.service.TagInfo;
import io.grpc.stub.StreamObserver;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MovieServiceImpl extends MovieServiceGrpc.MovieServiceImplBase {
    @Override
    public void getMovieProfile(MovieRequest request, StreamObserver<MovieProfileResponse> responseObserver) {
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        jedis.select(6);
        String movieInfo = "movInfo_"+request.getMovieId();
        String movieStatic = "movSt_"+request.getMovieId();
        MovieInfo info = (MovieInfo) SerializeUtil.deserializeToObj(jedis.get(movieInfo));
        double totalscore = Double.parseDouble(jedis.hget(movieStatic,"totalscore"));
        int totalcount = Integer.parseInt(jedis.hget(movieStatic,"totalcount"));
        double avg = totalscore/(double)totalcount;
        Set<TagInfo> topTags = info.getTopTags();
        List<RelMostTag> relMostTags = new ArrayList<>();
        Iterator<TagInfo> it = topTags.iterator();
        while(it.hasNext()){
            TagInfo tag = it.next();
            RelMostTag reltag = RelMostTag.newBuilder()
                    .setTagId(tag.getTagid())
                    .setTagName(tag.getName())
                    .setRelevance((float) tag.getRelevance())
                    .build();
            relMostTags.add(reltag);
        }
        Long def = 100000L;
        MovieProfileResponse response = MovieProfileResponse.newBuilder()
                .setMovieId(request.getMovieId())
                .setTitle(info.getTitle())
                .setTimestamp(def)
                .addAllGenre(info.getGenre())
                .addAllTag(relMostTags)
                .setAverageScore(avg)
                .setWatchedUser(totalcount)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
