package org.apache.hadoop.hbase.rsgroup;

import java.util.Comparator;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.net.HostAndPort;

/**
 * Constants used in rsgroup code
 * 
 * @author gokulvanan.v
 *
 */
public interface RSGroupConstants {

    String DEFAULT_GROUP = "default";
    String NAMESPACE_DESC_PROP_GROUP = "hbase.rsgroup.name";
    TableName RSGROUP_TABLE_NAME = TableName.valueOf(
            NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "rsgroup");
    byte[] RSGROUP_TABLE_NAME_BYTES = RSGROUP_TABLE_NAME.toBytes();
    String rsGroupZNode = "rsgroup";
    byte[] META_FAMILY_BYTES = Bytes.toBytes("m");
    byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");
    byte[] ROW_KEY = { 0 };
    Comparator<HostAndPort> HOST_AND_PORT_COMPARATOR = new Comparator<HostAndPort>() {

        @Override
        public int compare(HostAndPort o1, HostAndPort o2) {
            return (o1.toString().compareTo(o2.toString()));
        }
    };

}
