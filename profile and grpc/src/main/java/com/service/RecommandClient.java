package com.service;

import com.csvreader.CsvWriter;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.tensorflow.framework.DataType;
import org.tensorflow.framework.TensorProto;
import org.tensorflow.framework.TensorShapeProto;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import tensorflow.serving.Model;
import tensorflow.serving.Predict;
import tensorflow.serving.PredictionServiceGrpc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class RecommandClient {
    public static void main(String[] args) throws IOException {
        System.out.println("请选择以下两种功能:\n1) 输入uid获得5条电影推荐结果\n2) 输入uid和mid返回该用户对此电影打高分的概率");
        boolean invalid = true;
        while(invalid){
            Scanner sc = new Scanner(System.in);
            switch (sc.nextInt()){
                case 1:
                    invalid = false;
                    recommandMovies(sc);
                    break;
                case 2:
                    invalid = false;
                    recommandSingle(sc);
                    break;
                default:
                    System.out.println("请重新输入");
            }
        }
    }

    public static float getPri(PredictionServiceGrpc.PredictionServiceBlockingStub stub,double uavg_l,int ucnt_l,
                              double ufreq_l,String ufav_l,double urate_l,double mavg_l,int mcnt_l,int mrel_l,
                               double mrate_l,String mgre_l){
        // 创建请求
        Predict.PredictRequest.Builder predictRequestBuilder = Predict.PredictRequest.newBuilder();
        // 模型名称和模型方法名预设
        Model.ModelSpec.Builder modelSpecBuilder = Model.ModelSpec.newBuilder();
        modelSpecBuilder.setName("rank_model");
        modelSpecBuilder.setSignatureName("");
        predictRequestBuilder.setModelSpec(modelSpecBuilder);
        // 设置入参,访问默认是最新版本，如果需要特定版本可以使用tensorProtoBuilder.setVersionNumber方法

        TensorProto.Builder uavg_TensorProto = TensorProto.newBuilder();
        uavg_TensorProto.setDtype(DataType.DT_DOUBLE);
        uavg_TensorProto.addDoubleVal(uavg_l);
        TensorShapeProto.Builder uavg_ShapeBuilder = TensorShapeProto.newBuilder();
        uavg_ShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        uavg_TensorProto.setTensorShape(uavg_ShapeBuilder.build());

        TensorProto.Builder ucnt_TensorProto = TensorProto.newBuilder();
        ucnt_TensorProto.setDtype(DataType.DT_INT64);
        ucnt_TensorProto.addInt64Val(ucnt_l);
        TensorShapeProto.Builder ucnt_ShapeBuilder = TensorShapeProto.newBuilder();
        ucnt_ShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        ucnt_TensorProto.setTensorShape(ucnt_ShapeBuilder.build());

        TensorProto.Builder ufreq_TensorProto = TensorProto.newBuilder();
        ufreq_TensorProto.setDtype(DataType.DT_DOUBLE);
        ufreq_TensorProto.addDoubleVal(ufreq_l);
        TensorShapeProto.Builder ufreq_ShapeBuilder = TensorShapeProto.newBuilder();
        ufreq_ShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        ufreq_TensorProto.setTensorShape(ufreq_ShapeBuilder.build());

        TensorProto.Builder ufav_TensorProto = TensorProto.newBuilder();
        ufav_TensorProto.setDtype(DataType.DT_STRING);
        ufav_TensorProto.addStringVal(ByteString.copyFromUtf8(ufav_l));
        TensorShapeProto.Builder ufav_ShapeBuilder = TensorShapeProto.newBuilder();
        ufav_ShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        ufav_TensorProto.setTensorShape(ufav_ShapeBuilder.build());

        TensorProto.Builder urate_TensorProto = TensorProto.newBuilder();
        urate_TensorProto.setDtype(DataType.DT_DOUBLE);
        urate_TensorProto.addDoubleVal(urate_l);
        TensorShapeProto.Builder urateShapeBuilder = TensorShapeProto.newBuilder();
        urateShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        urate_TensorProto.setTensorShape(urateShapeBuilder.build());

        TensorProto.Builder mavg_TensorProto = TensorProto.newBuilder();
        mavg_TensorProto.setDtype(DataType.DT_DOUBLE);
        mavg_TensorProto.addDoubleVal(mavg_l);
        TensorShapeProto.Builder mavgShapeBuilder = TensorShapeProto.newBuilder();
        mavgShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        mavg_TensorProto.setTensorShape(mavgShapeBuilder.build());

        TensorProto.Builder mcnt_TensorProto = TensorProto.newBuilder();
        mcnt_TensorProto.setDtype(DataType.DT_INT64);
        mcnt_TensorProto.addInt64Val(mcnt_l);
        TensorShapeProto.Builder mcntShapeBuilder = TensorShapeProto.newBuilder();
        mcntShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        mcnt_TensorProto.setTensorShape(mcntShapeBuilder.build());

        TensorProto.Builder mrel_TensorProto = TensorProto.newBuilder();
        mrel_TensorProto.setDtype(DataType.DT_INT64);
        mrel_TensorProto.addInt64Val(mrel_l);
        TensorShapeProto.Builder mrelShapeBuilder = TensorShapeProto.newBuilder();
        mrelShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        mrel_TensorProto.setTensorShape(mrelShapeBuilder.build());

        TensorProto.Builder mrate_TensorProto = TensorProto.newBuilder();
        mrate_TensorProto.setDtype(DataType.DT_DOUBLE);
        mrate_TensorProto.addDoubleVal(mrate_l);
        TensorShapeProto.Builder mrateShapeBuilder = TensorShapeProto.newBuilder();
        mrateShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        mrate_TensorProto.setTensorShape(mrateShapeBuilder.build());

        TensorProto.Builder mgre_TensorProto = TensorProto.newBuilder();
        mgre_TensorProto.setDtype(DataType.DT_STRING);
        mgre_TensorProto.addStringVal(ByteString.copyFromUtf8(mgre_l));
        TensorShapeProto.Builder mgreShapeBuilder = TensorShapeProto.newBuilder();
        mgreShapeBuilder.addDim(TensorShapeProto.Dim.newBuilder().setSize(1));
        mgre_TensorProto.setTensorShape(mgreShapeBuilder.build());

        predictRequestBuilder.putInputs("user_average", uavg_TensorProto.build());
        predictRequestBuilder.putInputs("user_count", ucnt_TensorProto.build());
        predictRequestBuilder.putInputs("user_frequency", ufreq_TensorProto.build());
        predictRequestBuilder.putInputs("user_favor", ufav_TensorProto.build());
        predictRequestBuilder.putInputs("user_highrate", urate_TensorProto.build());

        predictRequestBuilder.putInputs("movie_average", mavg_TensorProto.build());
        predictRequestBuilder.putInputs("movie_count", mcnt_TensorProto.build());
        predictRequestBuilder.putInputs("movie_release", mrel_TensorProto.build());
        predictRequestBuilder.putInputs("movie_highrate", mrate_TensorProto.build());
        predictRequestBuilder.putInputs("movie_genre", mgre_TensorProto.build());


        // 访问并获取结果
        Predict.PredictResponse predictResponse = stub.withDeadlineAfter(10, TimeUnit.SECONDS).predict(predictRequestBuilder.build());
        Map<String, TensorProto> result = predictResponse.getOutputsMap();
        // CRF模型结果，发射概率矩阵和状态概率矩阵
//        System.out.println("预测值是:" + result.get("output_1").getFloatVal(0));
        return result.get("output_1").getFloatVal(0);
    }

    public static void recommandtoCSV() throws IOException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("192.168.43.218", 8502)
                .usePlaintext(true).build();
        // 这里使用block模式
        PredictionServiceGrpc.PredictionServiceBlockingStub stub = PredictionServiceGrpc.newBlockingStub(channel);
        CsvWriter csvWriter = new CsvWriter("D:\\Codes\\RecommendSystem-grpc\\callback_final.csv");
        csvWriter.writeRecord(new String[]{"uid","mid"});
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        Scanner filesc = new Scanner(new File("D:\\Codes\\RecommendSystem-grpc\\hot_final.csv"));
        filesc.nextLine();
        String allhot[] = new String[100];
        int count = 0;
        while(filesc.hasNext()){
            String hot_mid = filesc.nextLine();
            allhot[count++] = hot_mid;
        }
        Set<String> hotmid = new HashSet<>();
        Random random = new Random();
        while(hotmid.size() < 20){
            hotmid.add(allhot[random.nextInt(99)]);
        }
        Scanner sc = new Scanner(new File("D:\\Codes\\RecommendSystem-grpc\\uid_list.csv"));
        String all_user[] = sc.nextLine().split(",");
        int len = all_user.length;
        for(int i = 0; i < 50; i++){
            Profile userProfile = getUserProfile(Integer.parseInt(all_user[i]));
            Profile profile;
            Set<String> mids = new HashSet<>();
            mids.addAll(hotmid);
            jedis.select(7);
            Set<String> ucf = jedis.zrange("user_"+all_user[i],90,100);
            mids.addAll(ucf);
            jedis.select(8);
            Set<String> icf = jedis.zrange("user_"+all_user[i],90,100);
            mids.addAll(icf);
            Iterator<String> it = mids.iterator();
            List<Tuple> final_rank = new ArrayList<>();
            while (it.hasNext()){
                String mid = it.next();
                profile = getMovieProfile(mid,userProfile);
                float predict = getPri(stub, profile.getUser_average(), profile.getUser_count(), profile.getUser_frequency()
                        , profile.getUser_favor(), profile.getUser_highrate(),profile.getMovie_average(),
                        profile.getMovie_count(),profile.getMovie_release(),profile.getMovie_highrate(),
                        profile.getMovie_genre());
                Tuple tp = new Tuple(mid, (double) predict);
                final_rank.add(tp);
            }
            Collections.sort(final_rank,mapComparator);
            for(int j = 0; j < 5; j++){
                csvWriter.writeRecord(new String[]{all_user[i],final_rank.get(j).getElement()});
            }
        }
        csvWriter.close();
    }

    public static void recommandMovies(Scanner sc) throws FileNotFoundException {
        System.out.println("请输入用户id: ");
        int uid = sc.nextInt();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("192.168.43.218", 8502)
                .usePlaintext(true).build();
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        // 这里使用block模式
        PredictionServiceGrpc.PredictionServiceBlockingStub stub = PredictionServiceGrpc.newBlockingStub(channel);
        Profile userProfile = getUserProfile(uid);
        Profile profile;
        Set<String> mids = new HashSet<>();
        Scanner filesc = new Scanner(new File("D:\\Codes\\RecommendSystem-grpc\\hot_final.csv"));
        filesc.nextLine();
        String allhot[] = new String[100];
        int count = 0;
        while(filesc.hasNext()){
            String hot_mid = filesc.nextLine();
            allhot[count++] = hot_mid;
        }
        Set<String> hotmid = new HashSet<>();
        Random random = new Random();
        while(hotmid.size() < 10){
            hotmid.add(allhot[random.nextInt(99)]);
        }
        mids.addAll(hotmid);
        jedis.select(7);
        Set<String> ucf = jedis.zrange("user_"+uid,90,100);
        mids.addAll(ucf);
        jedis.select(8);
        Set<String> icf = jedis.zrange("user_"+uid,90,100);
        mids.addAll(icf);
        Iterator<String> it = mids.iterator();
        List<Tuple> final_rank = new ArrayList<>();
        while (it.hasNext()){
            String mid = it.next();
            profile = getMovieProfile(mid,userProfile);
            float predict = getPri(stub, profile.getUser_average(), profile.getUser_count(), profile.getUser_frequency()
                    , profile.getUser_favor(), profile.getUser_highrate(),profile.getMovie_average(),
                    profile.getMovie_count(),profile.getMovie_release(),profile.getMovie_highrate(),
                    profile.getMovie_genre());
            Tuple tp = new Tuple(mid, (double) predict);
            final_rank.add(tp);
        }
        Collections.sort(final_rank,mapComparator);
        System.out.println("推荐的电影如下: 格式为(movie_id, 推荐指数)");
        for(int i = 0; i < 5; i++){
            System.out.println(final_rank.get(i).getElement()+" "+final_rank.get(i).getScore());
        }
    }

    public static Comparator<Tuple> mapComparator = new Comparator<Tuple>() {
        public int compare(Tuple m1, Tuple m2) {
            if(m1.getScore() > m2.getScore()){
                return -1;
            }else if(m1.getScore() < m2.getScore()){
                return 1;
            }else{
                return 0;
            }
        }
    };

    public static Profile getMovieProfile(String mid,Profile userprofile){
        Profile result = userprofile;
        String mtotal,mcount,mgenre,mrate;
        Double dmavg,dmrate;
        long mrelease;
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        String movie_id = mid;
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
        result.setMovieProfile(Integer.parseInt(mcount)
                ,dmavg,dmrate,(int)mrelease,mgenre);
        return result;
    }

    public static void recommandSingle(Scanner sc){
        System.out.println("请输入用户id: ");
        int uid = sc.nextInt();
        System.out.println("请输入电影id: ");
        int mid = sc.nextInt();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("192.168.43.218", 8502)
                .usePlaintext(true).build();
        // 这里使用block模式
        PredictionServiceGrpc.PredictionServiceBlockingStub stub = PredictionServiceGrpc.newBlockingStub(channel);
        Profile profile = getSingleProfile(uid,mid);
        float predict = getPri(stub, profile.getUser_average(), profile.getUser_count(), profile.getUser_frequency()
                , profile.getUser_favor(), profile.getUser_highrate(),profile.getMovie_average(),
                profile.getMovie_count(),profile.getMovie_release(),profile.getMovie_highrate(),
                profile.getMovie_genre());
        System.out.println("用户对此电影打高分的概率为: "+predict);

    }

    public static Profile getUserProfile(int uid){
        String utotal,ucount,ufreq,ufavor,urate,mtotal,mcount,mgenre,mrate;
        Double duavg,dmavg,durate,dmrate;
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        jedis.select(1);
        String user_id = String.valueOf(uid);
        Map<String,String> user_profile = jedis.hgetAll("userSt_"+user_id);
        if(user_profile.isEmpty()){
            duavg = 0.0;
            durate = 0.0;
            ucount = "0";
            ufreq = "0.0";
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
        return new Profile(Integer.parseInt(ucount),duavg,Double.parseDouble(ufreq),durate,ufavor);
    }

    public static Profile getSingleProfile(int uid, int mid){
        String label;
        String utotal,ucount,ufreq,ufavor,urate,mtotal,mcount,mgenre,mrate;
        Double duavg,dmavg,durate,dmrate;
        long mrelease;
        Jedis jedis = RedisUtil.cli_pool("127.0.0.1",6379);
        jedis.select(1);
        String user_id = String.valueOf(uid);
        String movie_id = String.valueOf(mid);
        Map<String,String> user_profile = jedis.hgetAll("userSt_"+user_id);
        if(user_profile.isEmpty()){
            duavg = 0.0;
            durate = 0.0;
            ucount = "0";
            ufreq = "0.0";
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
        return new Profile(Integer.parseInt(ucount),duavg,
                Double.parseDouble(ufreq),durate,ufavor,Integer.parseInt(mcount)
                ,dmavg,dmrate,(int)mrelease,mgenre);
    }

}

