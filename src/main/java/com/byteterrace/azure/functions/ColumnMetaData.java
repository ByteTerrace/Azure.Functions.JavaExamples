package com.byteterrace.azure.functions;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;

public final class ColumnMetaData implements Serializable {
    private static final long serialVersionUID = 0L;

    private final DateTimeFormatter m_dateTimeFormatter;
    private final String m_name;
    private final int m_precision;
    private final int m_scale;
    private final int m_type;

    public DateTimeFormatter getDateTimeFormatter(){ return m_dateTimeFormatter; }
    public String getName(){ return m_name; }
    public int getPrecision(){ return m_precision; }
    public int getScale(){ return m_scale; }
    public int getType(){ return m_type; }

    ColumnMetaData(final String name, final int type, final int precision, final int scale, final DateTimeFormatter dateTimeFormatter) {
        m_dateTimeFormatter = dateTimeFormatter;
        m_name = name;
        m_precision = precision;
        m_scale = scale;
        m_type = type;
    }

    public static ColumnMetaData create(final String name, final int type, final int precision, final int scale, final DateTimeFormatter dateTimeFormatter) {
        final ColumnMetaData columnMetaData;

        switch (type) {
            /*
             * SQL Server supports numerous string literal formats for temporal types, hence sending them as varchar with approximate
             * precision(length) needed to send supported string literals. string literal formats supported by temporal types are available in MSDN
             * page on data types.
             */
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case microsoft.sql.Types.DATETIMEOFFSET:
                // The precision is just a number long enough to hold all types of temporal data, doesn't need to be exact.
                columnMetaData = new ColumnMetaData(name, type, 50, scale, dateTimeFormatter);
                break;

            // Redirect SQLXML as LONGNVARCHAR, SQLXML is not valid type in TDS
            case java.sql.Types.SQLXML:
                columnMetaData = new ColumnMetaData(name, java.sql.Types.LONGNVARCHAR, precision, scale, dateTimeFormatter);
                break;

            // Redirecting Float as Double based on data type mapping
            case java.sql.Types.FLOAT:
                columnMetaData = new ColumnMetaData(name, java.sql.Types.DOUBLE, precision, scale, dateTimeFormatter);
                break;

            // Redirecting BOOLEAN as BIT
            case java.sql.Types.BOOLEAN:
                columnMetaData = new ColumnMetaData(name, java.sql.Types.BIT, precision, scale, dateTimeFormatter);
                break;

            default:
                columnMetaData = new ColumnMetaData(name, type, precision, scale, dateTimeFormatter);
        }

        return columnMetaData;
    }
}
