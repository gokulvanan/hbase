package org.apache.hadoop.hbase.replication.rsgroup;


import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.RSGroupInfo.Builder;
import org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.ServerName;

import com.google.common.net.HostAndPort;

public enum ParseUtils {
    INSTANCE;

    public RSGroupInfo rsGroupFromProto(
            org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.RSGroupInfo rsGroupInfo) {
        RSGroupInfo output = new RSGroupInfo(rsGroupInfo.getName());
        Set<TableName> tables = new HashSet<>();
        for(org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.TableName tableProto : rsGroupInfo.getTablesList()){
        	tables.add(tableNameFromProto(tableProto));
        }
        output.addAllTables(tables);
        Set<HostAndPort> servers = new HashSet<>();
        for(org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.ServerName serverProto : rsGroupInfo.getServersList()){
        	servers.add(hostPortFromProto(serverProto));
        }
        output.addAllServers(servers);

        return output;
    }

    public org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.RSGroupInfo rsGroupToProto(
            RSGroupInfo rsGroupInfo) {
        Builder builder = org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.RSGroupInfo
                .newBuilder();
        builder.setName(rsGroupInfo.getName());
        int i = 0;
        for (TableName table : rsGroupInfo.getTables()) {
            builder.addTables(i++, tableNameToProto(table));
        }

        i = 0;
        for (HostAndPort hostPort : rsGroupInfo.getServers()) {
            builder.addServers(i++, hostPorttoProto(hostPort));
        }

        return builder.build();
    }

    public TableName tableNameFromProto(
            org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.TableName tableName) {
        return TableName.valueOf(tableName.getNamespace()
                .asReadOnlyByteBuffer(), tableName.getQualifier()
                .asReadOnlyByteBuffer());
    }

    public org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.TableName tableNameToProto(
            TableName table) {
        return org.apache.hadoop.hbase.replication.rsgroup.RSGroupAdminProto.TableName
                .newBuilder()
                .setNamespace(ByteStringer.wrap(table.getNamespace()))
                .setQualifier(ByteStringer.wrap(table.getQualifier())).build();
    }

    public HostAndPort hostPortFromProto(ServerName hostPort) {
        return HostAndPort
                .fromParts(hostPort.getHostName(), hostPort.getPort());
    }

    public ServerName hostPorttoProto(HostAndPort hostPort) {
        return ServerName.newBuilder().setHostName(hostPort.getHostText())
                .setPort(hostPort.getPort()).build();
    }
}
