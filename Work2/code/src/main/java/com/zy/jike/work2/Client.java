package com.zy.jike.work2;

import com.zy.jike.work2.utils.MyInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Client {
    public static void main(String[] args){
        try {
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L, new InetSocketAddress("127.0.0.1", 4411), new Configuration());
            String name = proxy.findName(20210123456789L);
            System.out.println(name);
            String name2 = proxy.findName(20210000000000L);
            System.out.println(name2);
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
