package com.service;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class CSVUtil {
    public static void main(String[] args) throws Exception {
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
//        outputToCSV(jedis,"D:\\Codes\\RecommendSystem-grpc\\ratings_test.csv","D:\\Codes\\RecommendSystem-grpc\\final_input.csv");
//        inputToCSV();
        System.out.println("finish file export");
    }

    public static void outputToCSV(Jedis jedis,String fromPath,String toPath) throws IOException {
        CsvReader csvReader = new CsvReader(fromPath);
        CsvWriter csvWriter = new CsvWriter(toPath);
        String[] headers = {"uid","mid","user_average","user_count","user_frequency","user_highrate","movie_release","movie_average",
                "movie_count","movie_highrate","user_favor","movie_genre","label"};
        csvWriter.writeRecord(headers);
        System.out.println(csvReader.readHeaders());
        String label;
        String utotal,ucount,ufreq,ufavor,urate,mtotal,mcount,mgenre,mrate;
        Double duavg,dmavg,durate,dmrate;
        Long mrelease;

        while (csvReader.readRecord()){
            jedis.select(1);
            String user_id = csvReader.get("userId");
            String movie_id = csvReader.get("movieId");
            Double score = Double.parseDouble(csvReader.get("rating"));
            if(score >= 4){
                label = "1";
            }else{
                label = "0";
            }
            Map<String,String> user_profile = jedis.hgetAll("userSt_"+user_id);
            if(user_profile.isEmpty()){
                duavg = 0.0;
                durate = 0.0;
                ucount = "0";
                ufreq = "0.0";
                urate = "0";
                ufavor = "(no genres listed)";
            }else{
                utotal = user_profile.get("total");
                ucount = user_profile.get("count");
                ufreq = user_profile.get("freq");
                ufavor = user_profile.get("favor");
                urate = user_profile.get("totalhigh");
                if(ufreq == "Infinity"){
                    ufreq = "1.0";
                }
                durate = Double.parseDouble(urate)/Double.parseDouble(ucount);
                duavg = Double.parseDouble(utotal)/Double.parseDouble(ucount);
            }
            jedis.select(6);
            MovieInfo movieInfo = (MovieInfo) SerializeUtil.deserializeToObj(jedis.get("movInfo_"+movie_id));
            Map<String,String> movie_statistic = jedis.hgetAll("movSt_"+movie_id);
            mrelease = movieInfo.getReleaseTime();
            mgenre = movieInfo.getGenre().get(0);
            mcount = movie_statistic.get("totalcount");
            mtotal = movie_statistic.get("totalscore");
            mrate = movie_statistic.get("totalhigh");
            if(Double.parseDouble(mcount) <= 0){
                dmavg = 0.0;
                dmrate = 0.0;
            }else{
                dmavg = Double.parseDouble(mtotal)/Integer.parseInt(mcount);
                dmrate = Double.parseDouble(mrate)/Integer.parseInt(mcount);
            }

            jedis.select(2);
            String[] content = {user_id,movie_id, String.valueOf(duavg),ucount,ufreq,String.valueOf(durate), String.valueOf(mrelease),
                    String.valueOf(dmavg),mcount,String.valueOf(dmrate),ufavor,mgenre,label};
            csvWriter.writeRecord(content);
        }
        csvWriter.close();
        csvReader.close();
    }


}
