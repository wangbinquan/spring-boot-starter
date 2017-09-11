/*
 *
 * Copyright BinquanWang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.wbq.spring.boot.autoconfigure.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class HbaseTool {
    private Log LOG = LogFactory.getLog(getClass());
    private org.apache.hadoop.conf.Configuration configuration = null;

    public HbaseTool(org.apache.hadoop.conf.Configuration configuration) throws IOException {
        this.configuration = configuration;
    }

    public boolean tableExist(String tableName) throws IOException {
        Admin admin = getAdmin();
        boolean flag = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return flag;
    }

    public boolean createTable(String tableName, String[] columnFamily, boolean rebuildTableIfExist) throws IOException {
        Admin admin = getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            if (rebuildTableIfExist) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            } else {
                return false;
            }
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String colFamily : columnFamily) {
            tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
        }
        admin.createTable(tableDescriptor);
        admin.close();
        return true;
    }

    public boolean createColumnFamily(String tableName, String[] columnFamily) throws IOException {
        Admin admin = getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        }
        admin.disableTable(TableName.valueOf(tableName));
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        for (String colFamily : columnFamily) {
            tableDescriptor.addFamily(new HColumnDescriptor(colFamily));
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.modifyTable(TableName.valueOf(tableName), tableDescriptor);
        admin.enableTable(TableName.valueOf(tableName));
        admin.close();
        return true;
    }

    public boolean insert(String tableName, String columnFamily, byte[] key, Map<byte[], byte[]> columnValue) throws IOException {
        Admin admin = getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        }
        byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
        Put put = new Put(key);
        for (Map.Entry<byte[], byte[]> column : columnValue.entrySet()) {
            put.addColumn(columnFamilyBytes, column.getKey(), column.getValue());
        }
        Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
        table.put(put);
        table.close();
        return true;
    }

    public byte[][] get(String tableName, String columnFamily, byte[] key, byte[][] columnsToSearch) throws IOException {
        Admin admin = getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            return null;
        }
        Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
        Get get = new Get(key);
        byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
        Result result = table.get(get);
        byte[][] results = new byte[columnsToSearch.length][];
        for (int i = 0; i < columnsToSearch.length; i++) {
            results[i] = result.getValue(columnFamilyBytes, columnsToSearch[i]);
        }
        table.close();
        return results;
    }

    public boolean delete(String tableName, String columnFamily, byte[] key) throws IOException {
        Admin admin = getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            return false;
        }
        Delete delete = new Delete(key);
        if(columnFamily != null && columnFamily != "") {
            delete.addFamily(Bytes.toBytes(columnFamily));
        }
        Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
        table.delete(delete);
        table.close();
        return true;
    }


    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(configuration);
    }

    public Admin getAdmin() throws IOException {
        return getConnection().getAdmin();
    }
}
