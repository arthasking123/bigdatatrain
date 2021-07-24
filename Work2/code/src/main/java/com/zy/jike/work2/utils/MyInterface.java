package com.zy.jike.work2.utils;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol{
    long versionID = 1L;
    String findName(Long studentId);
}
