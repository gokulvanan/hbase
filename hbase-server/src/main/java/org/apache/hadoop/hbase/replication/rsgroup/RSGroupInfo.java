package org.apache.hadoop.hbase.replication.rsgroup;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.TableName;
import org.apache.log4j.Logger;

import com.google.common.net.HostAndPort;

/**
 * Representation of RSGrouop for client response Stores set of servers with set
 * of tables for a rsgroup name
 */
public class RSGroupInfo {

    private static final Logger LOG = Logger.getLogger(RSGroupInfo.class);
    private final String name;
    // Keep servers in a sorted set so has an expected ordering when displayed.
    private final SortedSet<HostAndPort> servers;
    // Keep tables sorted too.
    private final SortedSet<TableName> tables;
    
    private static Comparator<HostAndPort> HOST_AND_PORT_COMPARATOR = new Comparator<HostAndPort>() {

        @Override
        public int compare(HostAndPort o1, HostAndPort o2) {
            return (o1.toString().compareTo(o2.toString()));
        }
    };

    public RSGroupInfo(String name) {
        this(name, new TreeSet<HostAndPort>(), new TreeSet<TableName>());
    }

    RSGroupInfo(String name, SortedSet<HostAndPort> servers,
            SortedSet<TableName> tables) {
        this.name = name;
        this.servers = new TreeSet<>(HOST_AND_PORT_COMPARATOR);
        this.servers.addAll(servers);
        this.tables = new TreeSet<>(tables);
    }

    public RSGroupInfo(RSGroupInfo src) {
        this(src.getName(), src.servers, src.tables);
    }

    /**
     * Get group name.
     */
    public String getName() {
        return name;
    }

    /**
     * Adds the given server to the group.
     */
    public void addServer(HostAndPort hostPort) {
        servers.add(hostPort);
    }

    /**
     * Adds the given servers to the group.
     */
    public void addAllServers(Collection<HostAndPort> hostPort) {
        servers.addAll(hostPort);
    }

    /**
     * @param hostPort
     *            hostPort of the server
     * @return true, if a server with hostPort is found
     */
    public boolean containsServer(HostAndPort hostPort) {
        return servers.contains(hostPort);
    }

    /**
     * Get list of servers.
     */
    public Set<HostAndPort> getServers() {
        return servers;
    }

    /**
     * Remove given server from the group.
     */
    public boolean removeServer(HostAndPort hostPort) {
        return servers.remove(hostPort);
    }

    /**
     * Get set of tables that are members of the group.
     */
    public SortedSet<TableName> getTables() {
        return tables;
    }

    public void addTable(TableName table) {
        tables.add(table);
    }

    public void addAllTables(Collection<TableName> arg) {
        tables.addAll(arg);
    }

    public boolean containsTable(TableName table) {
        return tables.contains(table);
    }

    public boolean removeTable(TableName table) {
        return tables.remove(table);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Name:");
        sb.append(this.name);
        sb.append(", ");
        sb.append(" Servers:");
        sb.append(this.servers);
        sb.append(" Tables:");
        sb.append(this.tables);
        return sb.toString();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RSGroupInfo RSGroupInfo = (RSGroupInfo) o;

        if (!name.equals(RSGroupInfo.name)) {
            return false;
        }
        if (!servers.equals(RSGroupInfo.servers)) {
            return false;
        }
        if (!tables.equals(RSGroupInfo.tables)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = servers.hashCode();
        result = 31 * result + tables.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    public RSGroupInfo immutableCopy() {
        RSGroupInfo copy = new RSGroupInfo(this);
        return copy;
    }

    public static void main(String[] args) {
        RSGroupInfo info = new RSGroupInfo("test");
        info.containsServer(HostAndPort.fromString("10.33.109.167:16020"));
        info.addServer(HostAndPort.fromString("10.33.109.167:16020"));
        info.containsServer(HostAndPort.fromString("10.33.109.167:16020"));
        System.out.println("Ran without issues");

    }
}