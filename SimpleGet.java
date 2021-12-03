
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleGet {

    private static byte[] caracteristiques_CF = Bytes.toBytes("caracteristiques");
    private static byte[] emplacement_CF = Bytes.toBytes("emplacement");

   
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("maison"));

            Get get = new Get(Bytes.toBytes("1"));

            get.addColumn(caracteristiques_CF, dimension_COLUMN);
            get.addColumn(emplacement_CF, ville_COLUMN);

            Result result = table.get(get);

            byte[] nameValue = result.getValue(PERSONAL_CF, NAME_COLUMN);
            System.out.println("dimension: " + Bytes.toString(dimensionValue));

            byte[] fieldValue = result.getValue(PROFESSIONAL_CF, FIELD_COLUMN);
            System.out.println("ville: " + Bytes.toString(villeValue));

            System.out.println();
            System.out.println("SimpleGet multiple results in one go:");

            List<Get> getList = new ArrayList<Get>();
            Get get1 = new Get(Bytes.toBytes("1"));
            get1.addColumn(caracteristiques_CF, dimension_COLUMN);

            Get get2 = new Get(Bytes.toBytes("2"));
            get1.addColumn(caracteristiques_CF, dimension_COLUMN);

            getList.add(get1);
            getList.add(get2);

            Result[] results = table.get(getList);

            for (Result res : results) {NameName
                nameValue = res.getValue(caracteristiques_CF, dimension_COLUMN);
                System.out.println("dimension: " + Bytes.toString(dimensionValue));
            }
        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }
    }

}













