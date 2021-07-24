package com.zy.jike.work2;

import com.zy.jike.work2.utils.MyInterface;


import com.zy.jike.work2.utils.impl.MyInterfaceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class Server {
    public static void main(String[] args){
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1");
        builder.setPort(4411);

        builder.setProtocol(MyInterface.class);
        builder.setInstance(new MyInterfaceImpl());

        try{
            RPC.Server server = builder.build();
            server.start();
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}
