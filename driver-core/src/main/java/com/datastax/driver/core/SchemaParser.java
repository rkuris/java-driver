package com.datastax.driver.core;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.SchemaElement.*;

abstract class SchemaParser {
    private static final Logger logger = LoggerFactory.getLogger(SchemaParser.class);

    abstract void refresh(Metadata metadata,
                          SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                          Connection connection, VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException;

    static SchemaParser forVersion(VersionNumber cassandraVersion) {
        return V2_PARSER;
    }

    private static SchemaParser V2_PARSER = new SchemaParser() {
        private static final String SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces";
        private static final String SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies";
        private static final String SELECT_COLUMNS = "SELECT * FROM system.schema_columns";
        private static final String SELECT_USERTYPES = "SELECT * FROM system.schema_usertypes";
        private static final String SELECT_FUNCTIONS = "SELECT * FROM system.schema_functions";
        private static final String SELECT_AGGREGATES = "SELECT * FROM system.schema_aggregates";

        @Override
        void refresh(Metadata metadata,
                     SchemaElement targetType, String targetKeyspace, String targetName, List<String> targetSignature,
                     Connection connection, VersionNumber cassandraVersion)
            throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException {

            boolean isSchemaOrKeyspace = (targetType == null || targetType == KEYSPACE);
            ProtocolVersion protocolVersion = metadata.cluster.protocolVersion();

            String whereClause = "";
            if (targetType != null) {
                whereClause = " WHERE keyspace_name = '" + targetKeyspace + '\'';
                if (targetType == TABLE)
                    whereClause += " AND columnfamily_name = '" + targetName + '\'';
                else if (targetType == TYPE)
                    whereClause += " AND type_name = '" + targetName + '\'';
                else if (targetType == FUNCTION)
                    whereClause += " AND function_name = '" + targetName + "' AND signature = " + DataType.LIST_OF_TEXT.format(targetSignature);
                else if (targetType == AGGREGATE)
                    whereClause += " AND aggregate_name = '" + targetName + "' AND signature = " + DataType.LIST_OF_TEXT.format(targetSignature);
            }

            ResultSetFuture ksFuture = null,
                udtFuture = null,
                cfFuture = null,
                colsFuture = null,
                functionsFuture = null,
                aggregatesFuture = null;

            if (isSchemaOrKeyspace)
                ksFuture = queryAsync(SELECT_KEYSPACES + whereClause, connection, protocolVersion);

            if (isSchemaOrKeyspace && supportsUdts(cassandraVersion) || targetType == TYPE)
                udtFuture = queryAsync(SELECT_USERTYPES + whereClause, connection, protocolVersion);

            if (isSchemaOrKeyspace || targetType == TABLE) {
                cfFuture = queryAsync(SELECT_COLUMN_FAMILIES + whereClause, connection, protocolVersion);
                colsFuture = queryAsync(SELECT_COLUMNS + whereClause, connection, protocolVersion);
            }

            if ((isSchemaOrKeyspace && supportsUdfs(cassandraVersion) || targetType == FUNCTION))
                functionsFuture = queryAsync(SELECT_FUNCTIONS + whereClause, connection, protocolVersion);

            if (isSchemaOrKeyspace && supportsUdfs(cassandraVersion) || targetType == AGGREGATE)
                aggregatesFuture = queryAsync(SELECT_AGGREGATES + whereClause, connection, protocolVersion);

            ResultSet ks = get(ksFuture);
            Map<String, List<Row>> udtDefs = groupByKeyspace(get(udtFuture));
            Map<String, List<Row>> cfDefs = groupByKeyspace(get(cfFuture));
            Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> colsDefs = groupByKeyspaceAndCf(get(colsFuture), cassandraVersion);
            Map<String, List<Row>> functionDefs = groupByKeyspace(get(functionsFuture));
            Map<String, List<Row>> aggregateDefs = groupByKeyspace(get(aggregatesFuture));

            if (targetType == null || targetType == KEYSPACE) { // Refresh one or all keyspaces
                assert ks != null;
                Set<String> addedKs = new HashSet<String>();
                for (Row ksRow : ks) {
                    String ksName = ksRow.getString("keyspace_name");
                    KeyspaceMetadata ksm = KeyspaceMetadata.build(ksRow, udtDefs.get(ksName), cassandraVersion);

                    if (cfDefs.containsKey(ksName)) {
                        buildTableMetadata(ksm, cfDefs.get(ksName), colsDefs.get(ksName), cassandraVersion);
                    }
                    if (functionDefs.containsKey(ksName)) {
                        buildFunctionMetadata(ksm, functionDefs.get(ksName), cassandraVersion);
                    }
                    if (aggregateDefs.containsKey(ksName)) {
                        buildAggregateMetadata(ksm, aggregateDefs.get(ksName), cassandraVersion, protocolVersion);
                    }
                    addedKs.add(ksName);
                    metadata.keyspaces.put(ksName, ksm);
                }

                // If keyspace is null, it means we're rebuilding from scratch, so
                // remove anything that was not just added as it means it's a dropped keyspace
                if (targetKeyspace == null) {
                    Iterator<String> iter = metadata.keyspaces.keySet().iterator();
                    while (iter.hasNext()) {
                        if (!addedKs.contains(iter.next()))
                            iter.remove();
                    }
                }
            } else {
                assert targetKeyspace != null;
                KeyspaceMetadata ksm = metadata.keyspaces.get(targetKeyspace);

                // If we update a keyspace we don't know about, something went
                // wrong. Log an error an schedule a full schema rebuilt.
                if (ksm == null) {
                    logger.error(String.format("Asked to rebuild %s %s.%s but I don't know keyspace %s", targetType, targetKeyspace, targetName, targetKeyspace));
                    metadata.cluster.submitSchemaRefresh(null, null, null, null);
                    return;
                }

                switch (targetType) {
                    case TABLE:
                        if (cfDefs.containsKey(targetKeyspace))
                            buildTableMetadata(ksm, cfDefs.get(targetKeyspace), colsDefs.get(targetKeyspace), cassandraVersion);
                        break;
                    case TYPE:
                        if (udtDefs.containsKey(targetKeyspace))
                            ksm.addUserTypes(udtDefs.get(targetKeyspace), cassandraVersion);
                        break;
                    case FUNCTION:
                        if (functionDefs.containsKey(targetKeyspace))
                            buildFunctionMetadata(ksm, functionDefs.get(targetKeyspace), cassandraVersion);
                        break;
                    case AGGREGATE:
                        if (functionDefs.containsKey(targetKeyspace))
                            buildAggregateMetadata(ksm, aggregateDefs.get(targetKeyspace), cassandraVersion, protocolVersion);
                        break;
                    default:
                        logger.warn("Unexpected element type to rebuild: {}", targetType);
                }
            }
        }

        private boolean supportsUdts(VersionNumber cassandraVersion) {
            return cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1);
        }

        private boolean supportsUdfs(VersionNumber cassandraVersion) {
            return cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 2);
        }

        private Map<String, List<Row>> groupByKeyspace(ResultSet rs) {
            if (rs == null)
                return Collections.emptyMap();

            Map<String, List<Row>> result = new HashMap<String, List<Row>>();
            for (Row row : rs) {
                String ksName = row.getString("keyspace_name");
                List<Row> l = result.get(ksName);
                if (l == null) {
                    l = new ArrayList<Row>();
                    result.put(ksName, l);
                }
                l.add(row);
            }
            return result;
        }

        private Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> groupByKeyspaceAndCf(ResultSet rs, VersionNumber cassandraVersion) {
            if (rs == null)
                return Collections.emptyMap();

            Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> result =
                new HashMap<String, Map<String, Map<String, ColumnMetadata.Raw>>>();
            for (Row row : rs) {
                String ksName = row.getString("keyspace_name");
                String cfName = row.getString("columnfamily_name");
                Map<String, Map<String, ColumnMetadata.Raw>> colsByCf = result.get(ksName);
                if (colsByCf == null) {
                    colsByCf = new HashMap<String, Map<String, ColumnMetadata.Raw>>();
                    result.put(ksName, colsByCf);
                }
                Map<String, ColumnMetadata.Raw> l = colsByCf.get(cfName);
                if (l == null) {
                    l = new HashMap<String, ColumnMetadata.Raw>();
                    colsByCf.put(cfName, l);
                }
                ColumnMetadata.Raw c = ColumnMetadata.Raw.fromRow(row, cassandraVersion);
                l.put(c.name, c);
            }
            return result;
        }

        private void buildTableMetadata(KeyspaceMetadata ksm, List<Row> cfRows, Map<String, Map<String, ColumnMetadata.Raw>> colsDefs, VersionNumber cassandraVersion) {
            for (Row cfRow : cfRows) {
                String cfName = cfRow.getString(TableMetadata.CF_NAME);
                try {
                    Map<String, ColumnMetadata.Raw> cols = colsDefs == null ? null : colsDefs.get(cfName);
                    if (cols == null || cols.isEmpty()) {
                        if (cassandraVersion.getMajor() >= 2) {
                            // In C* >= 2.0, we should never have no columns metadata because at the very least we should
                            // have the metadata corresponding to the default CQL metadata. So if we don't have any columns,
                            // that can only mean that the table got creating concurrently with our schema queries, and the
                            // query for columns metadata reached the node before the table was persisted while the table
                            // metadata one reached it afterwards. We could make the query to the column metadata sequential
                            // with the table metadata instead of in parallel, but it's probably not worth making it slower
                            // all the time to avoid this race since 1) it's very very uncommon and 2) we can just ignore the
                            // incomplete table here for now and it'll get updated next time with no particular consequence
                            // (if the table creation was concurrent with our querying, we'll get a notifciation later and
                            // will reupdate the schema for it anyway). See JAVA-320 for why we need this.
                            continue;
                        } else {
                            // C* 1.2 don't persists default CQL metadata, so it's possible not to have columns (for thirft
                            // tables). But in that case TableMetadata.build() knows how to handle it.
                            cols = Collections.<String, ColumnMetadata.Raw>emptyMap();
                        }
                    }
                    TableMetadata.build(ksm, cfRow, cols, cassandraVersion);
                } catch (RuntimeException e) {
                    // See ControlConnection#refreshSchema for why we'd rather not probably this further
                    logger.error(String.format("Error parsing schema for table %s.%s: "
                            + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\") will be missing or incomplete",
                        ksm.getName(), cfName, ksm.getName(), cfName), e);
                }
            }
        }

        private void buildFunctionMetadata(KeyspaceMetadata ksm, List<Row> rows, VersionNumber cassandraVersion) {
            for (Row row : rows)
                FunctionMetadata.build(ksm, row, cassandraVersion);
        }

        private void buildAggregateMetadata(KeyspaceMetadata ksm, List<Row> rows, VersionNumber cassandraVersion, ProtocolVersion protocolVersion) {
            for (Row row : rows)
                AggregateMetadata.build(ksm, row, cassandraVersion, protocolVersion);
        }
    };

    private static ResultSetFuture queryAsync(String query, Connection connection, ProtocolVersion protocolVersion) throws ConnectionException, BusyConnectionException {
        DefaultResultSetFuture future = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query(query));
        connection.write(future);
        return future;
    }

    private static ResultSet get(ResultSetFuture future) throws InterruptedException, ExecutionException {
        return (future == null) ? null : future.get();
    }
}
