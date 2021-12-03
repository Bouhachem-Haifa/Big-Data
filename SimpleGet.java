
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
            get.addColumn(caracteristiques_CF, nombre_de_chambre_COLUMN);
            get.addColumn(emplacement_CF, ville_COLUMN);
            get.addColumn(emplacement_CF, pays_COLUMN);


            Result result = table.get(get);

            byte[] dimensionValue = result.getValue(caracteristiques_CF, dimension_COLUMN);
            System.out.println("Name: " + Bytes.toString(dimensionValue));

            byte[] nombre_de_chambreValue = result.getValue(caracteristiques_CF, nombre_de_chambre_COLUMN);
            System.out.println("Field: " + Bytes.toString(nombre_de_chambreValue));
            
            byte[] villeValue = result.getValue(emplacement_CF, ville_COLUMN);
            System.out.println("Name: " + Bytes.toString(villeValue));

            byte[] paysValue = result.getValue(emplacement_CF, pays_COLUMN);
            System.out.println("Field: " + Bytes.toString(paysValue));

            System.out.println();
            System.out.println("SimpleGet multiple results in one go:");

            List<Get> getList = new ArrayList<Get>();
            Get get1 = new Get(Bytes.toBytes("2"));
            get1.addColumn(caracteristiques_CF, nombre_de_chambre_COLUMN);

            Get get2 = new Get(Bytes.toBytes("3"));
            get1.addColumn(caracteristiques_CF, nombre_de_chambre_COLUMN);

            getList.add(get1);
            getList.add(get2);

            Result[] results = table.get(getList);

            for (Result res : results) {
                nombre_de_chambreValue = res.getValue(caracteristiques_CF, nombre_de_chambre_COLUMN);
                System.out.println("nombre_de_chambre: " + Bytes.toString(nombre_de_chambreValue));
            }
        } finally {
            connection.close();
            if (table != null) {
                table.close();
            }
        }
    }

}













