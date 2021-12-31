package com.service;

import java.io.*;

public class SerializeUtil {
    public static String serializeToString(Object obj) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream;
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        String string = byteArrayOutputStream.toString("ISO-8859-1");
        objectOutputStream.close();
        byteArrayOutputStream.close();
        return string;
    }

    public static Object deserializeToObj(String str){
        ByteArrayInputStream buf = null;
        ObjectInputStream objInput = null;
        try {
            buf = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));
            objInput = new ObjectInputStream(buf);
            return objInput.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }
}
