package com.zy.bigdata.work3.hbase_syllabus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.IOException;

public class HBaseDemo {
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","jikehadoop01,jikehadoop02,jikehadoop03");
    }

    public  static  boolean  isExist(String tableName) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Admin admin = conc.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName,String... columnFamily) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Admin admin = conc.getAdmin();
        if (isExist(tableName)){
            System.out.println(tableName+"表已经存在！");
        }else{
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            for(String cf : columnFamily){
                htd.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(htd);
            System.out.println(tableName+"表创建成功！");
        }

    }

    public static void deleteTable(String tableName) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Admin admin = conc.getAdmin();

        if(isExist(tableName)){
            if(!admin.isTableDisabled(TableName.valueOf(tableName))){
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName+"表删除成功！");
        }else {
            System.out.println(tableName+"表不存在！");
        }
    }

    public static void addRow(String tableName,String rowKey, String cf,String column,String value) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Table table = conc.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);

    }

    public static void  deleteRow(String tableName,String rowKey) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Table table = conc.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public static void getAllRows(String tableName) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Table table = conc.getTable(TableName.valueOf(tableName));
        Scan scan =new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            System.out.println("正在遍历"+Bytes.toString(result.getRow())+"这条行键的所有单元格信息");
            Cell [] cells = result.rawCells();
            for(Cell cell:cells){
                System.out.print("行键"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.print("列族"+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.print("列字段"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("单元格值"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("当前行键信息遍历结束！");
        }
    }

    public static void getRow(String tableName,String rowKey,String cf, String column) throws IOException {
        Connection conc = ConnectionFactory.createConnection(conf);
        Table table = conc.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column));
        Result result = table.get(get);
        System.out.println(result);
    }


    public static void main(String[] Args) throws IOException, JSONException {
        createTable("zhangyang:student","info","score");

        String tableName = "zhangyang:student";
        String insertString = "[{\"name\":\"Tom\",\"info\":{\"student_id\":\"20210000000001\",\"class\":\"1\"},\"score\":{\"understanding\":\"75\",\"programming\":\"82\"}},{\"name\":\"Jerry\",\"info\":{\"student_id\":\"20210000000002\",\"class\":\"1\"},\"score\":{\"understanding\":\"85\",\"programming\":\"67\"}},{\"name\":\"Jack\",\"info\":{\"student_id\":\"20210000000003\",\"class\":\"2\"},\"score\":{\"understanding\":\"80\",\"programming\":\"80\"}},{\"name\":\"Rose\",\"info\":{\"student_id\":\"20210000000004\",\"class\":\"2\"},\"score\":{\"understanding\":\"60\",\"programming\":\"61\"}},{\"name\":\"Zhangyang\",\"info\":{\"student_id\":\"G20200343080434\",\"class\":\"2\"},\"score\":{\"understanding\":\"60\",\"programming\":\"61\"}}]";
        JSONArray dataArray = new JSONArray(insertString);
        for(int i=0;i<dataArray.length();i++){
            JSONObject jsonObj = dataArray.getJSONObject(i);
            String name = jsonObj.getString("name");
            JSONObject info = jsonObj.getJSONObject("info");
            JSONObject score = jsonObj.getJSONObject("score");
            addRow(tableName,name,"info","student_id", info.getString("student_id"));
            addRow(tableName,name,"info","class", info.getString("class"));
            addRow(tableName,name,"score","understanding", score.getString("understanding"));
            addRow(tableName,name,"score","programming", score.getString("programming"));
        }
        getAllRows(tableName);
//        deleteRow(tableName,"Tom");
//        getAllRows(tableName);
//        deleteTable(tableName);
    }
}
