package com.byteterrace.azure.functions;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerResource;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.OffsetTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqlServerBulkDataT implements ISQLServerBulkData {
    private final Iterator<List<String>> m_csvRows;
    private final Map<Integer, ColumnMetaData> m_metaData;

    public SqlServerBulkDataT(final Iterable<List<String>> csvRows) {
        m_csvRows = csvRows.iterator();
        m_metaData = null;
    }

    @Override
    public String getColumnName(final int column) {
        return m_metaData.get(column).getName();
    }
    @Override
    public Set<Integer> getColumnOrdinals() {
        return m_metaData.keySet();
    }
    @Override
    public int getColumnType(final int column) {
        return m_metaData.get(column).getType();
    }
    @Override
    public int getPrecision(final int column) {
        return m_metaData.get(column).getPrecision();
    }
    @Override
    public Object[] getRowData() throws SQLException {
        final Iterator<List<String>> csvRowIterator = m_csvRows;
        final List<String> row = csvRowIterator.next();
        final Object[] columnValues = new Object[row.size()];

        for (Map.Entry<Integer, ColumnMetaData> pair : m_metaData.entrySet()) {
            final int columnIndex = (pair.getKey() - 1);
            final ColumnMetaData columnMetaData = pair.getValue();

            try {
                switch (columnMetaData.getType()){
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.TIMESTAMP_WITH_TIMEZONE: {
                        final OffsetTime offsetTimeValue;

                        if (columnMetaData.getDateTimeFormatter() != null) {
                            offsetTimeValue = OffsetTime.parse(row.get(columnIndex).toString(), columnMetaData.getDateTimeFormatter());
                        }
                        else {
                            offsetTimeValue = OffsetTime.parse(row.get(columnIndex).toString());
                        }

                        columnValues[columnIndex] = offsetTimeValue;

                        break;
                    }
                    case Types.NULL: {
                        columnValues[columnIndex] = null;
                        break;
                    }
                    default: {
                        columnValues[columnIndex] = row.get(columnIndex);
                        break;
                    }
                }
            } catch (final IllegalArgumentException illegalArgumentException) {
                final MessageFormat form = new MessageFormat(getSQLServerExceptionErrorMsg("R_errorConvertingValue"));
                final String message = form.format(new Object[]{
                    "'" + row.get(columnIndex) + "'",
                    JDBCType.valueOf(columnMetaData.getType()).getName()
                });

                throw new SQLException(message, null, 0, illegalArgumentException);
            } catch (final ArrayIndexOutOfBoundsException arrayOutOfBoundsException) {
                throw new SQLException(getSQLServerExceptionErrorMsg("R_schemaMismatch"), arrayOutOfBoundsException);
            }
        }

        return columnValues;
    }
    private String getSQLServerExceptionErrorMsg(final String type) {
        return SQLServerResource.getBundle("com.microsoft.sqlserver.jdbc.SQLServerResource").getString(type);
    }
    @Override
    public int getScale(final int column) {
        return m_metaData.get(column).getScale();
    }
    @Override
    public boolean next() throws SQLException {
        return m_csvRows.hasNext();
    }
}
