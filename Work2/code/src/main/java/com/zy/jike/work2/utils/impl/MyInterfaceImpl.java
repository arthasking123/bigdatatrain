package com.zy.jike.work2.utils.impl;

import com.zy.jike.work2.utils.MyInterface;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MyInterfaceImpl implements MyInterface {
    Map<Long, String> studentMap = new HashMap<Long, String>(){{
        put(20210123456789L,"心心");
    }};
    @Override
    public String findName(Long studentId){
        return studentMap.get(studentId);
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion){
        return MyInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String var1, long var2, int var4) throws IOException{
        return null;
    }

}
