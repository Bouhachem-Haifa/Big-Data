import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimplePut {

    private static byte[] caracteristiques_CF = Bytes.toBytes("personal");
    private static byte[] emplacement_CF = Bytes.toBytes("emplacement");

    private static byte[] dimension_COLUMN = Bytes.toBytes("dimension");
    private static byte[] nombre_de_chambre_COLUMN = Bytes.toBytes("nombre_de_chambre");
    private static byte[] ville_COLUMN = Bytes.toBytes("ville");
    private static byte[] pays_COLUMN = Bytes.toBytes("pays");


    
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("maison"));

            Put put1 = new Put(Bytes.toBytes("1"));

            put1.addColumn(caracteristiques_CF,dimension_COLUMN , Bytes.toBytes("500"));
            put1.addColumn(caracteristiques_CF, nombre_de_chambre_COLUMN, Bytes.toBytes("3"));
            put1.addColumn(emplacement_CF, ville_COLUMN, Bytes.toBytes("djerba"));
            put1.addColumn(emplacement_CF, pays_COLUMN, Bytes.toBytes("tunisie"));
            

            table.put(put1);

            System.out.println("Inserted row for Mike Jones");

            Put put2 = new Put(Bytes.toBytes("2"));

            put2.addColumn(caracteristiques_CF,dimension_COLUMN , Bytes.toBytes("600"));
            put2.addColumn(caracteristiques_CF, nombre_de_chambre_COLUMN, Bytes.toBytes("4"));
            put2.addColumn(emplacement_CF, ville_COLUMN, Bytes.toBytes("Tunis"));
            put2.addColumn(emplacement_CF, pays_COLUMN, Bytes.toBytes("tunisie"));
            
            List<Put> putList = new ArrayList<Put>();
            putList.add(put1);
            putList.add(put2);

            table.put(putList);

            System.out.println("Inserted rows for Hillary Clinton and Donald Trump");
        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }
    }
}






















