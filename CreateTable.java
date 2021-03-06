import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
public class CreateTable {
public static void main(String[] args) throws IOException {
Configuration conf = HBaseConfiguration.create();
Connection connection = ConnectionFactory.createConnection(conf);
try {
Admin admin = connection.getAdmin();
HTableDescriptor tableName = new
HTableDescriptor(TableName.valueOf("maison"));
tableName.addFamily(new HColumnDescriptor("caracteristiques"));
tableName.addFamily(new HColumnDescriptor("emplacement"));
if (!admin.tableExists(tableName.getTableName())) {
System.out.print("Creating the maison table. ");
admin.createTable(tableName);
System.out.println("Done.");
} else {
System.out.println("Table already exists");
}
} finally {
connection.close();
}
}
}