/*
 * Hibernate Utilities
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the LICENSE file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package com.tickerfit.domain.utility;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.internal.FormatStyle;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionEventListenerManager;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.Configurable;
import org.hibernate.id.ExportableColumn;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.id.PersistentIdentifierGenerator;
import org.hibernate.id.enhanced.AccessCallback;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.id.enhanced.OptimizerFactory;
import org.hibernate.id.enhanced.StandardOptimizerDescriptor;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.jdbc.AbstractReturningWork;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PrimaryKey;
import org.hibernate.mapping.Table;
import org.hibernate.service.ServiceRegistry;

import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.jboss.logging.Logger;

/**
 * An modified version of the enhanced version of table-based id generation provided by
 * Hibernate.
 * <p/>
 *
 * NOTE: Can only be used as a generator on entities that implement {@link EntityGroupingIdentifier}
 *
 * <p/>
 * Configuration parameters:
 * <table>
 * 	 <tr>
 *     <td><b>NAME</b></td>
 *     <td><b>DEFAULT</b></td>
 *     <td><b>DESCRIPTION</b></td>
 *   </tr>
 *   <tr>
 *     <td>{@link #TABLE_PARAM}</td>
 *     <td>{@link #DEF_TABLE}</td>
 *     <td>The name of the table to use to store/retrieve values</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #VALUE_COLUMN_PARAM}</td>
 *     <td>{@link #DEF_VALUE_COLUMN}</td>
 *     <td>The name of column which holds the sequence value for the given segment</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #SEGMENT_COLUMN_PARAM}</td>
 *     <td>{@link #DEF_SEGMENT_COLUMN}</td>
 *     <td>The name of the column which holds the segment key</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #SEGMENT_LENGTH_PARAM}</td>
 *     <td>{@link #DEF_SEGMENT_LENGTH}</td>
 *     <td>The data length of the {@link #SEGMENT_COLUMN_PARAM} column; used for schema creation</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #INITIAL_PARAM}</td>
 *     <td>{@link #DEFAULT_INITIAL_VALUE}</td>
 *     <td>The initial value to be stored for the given segment</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #INCREMENT_PARAM}</td>
 *     <td>{@link #DEFAULT_INCREMENT_SIZE}</td>
 *     <td>The increment size for the underlying segment; see the discussion on {@link Optimizer} for more details.</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #OPT_PARAM}</td>
 *     <td><i>depends on defined increment size</i></td>
 *     <td>Allows explicit definition of which optimization strategy to use</td>
 *   </tr>
 * </table>
 *
 * @author Steve Ebersole
 * @author Greg Balmer
 */

public class PrefixedEntitySequenceIdGenerator implements PersistentIdentifierGenerator, Configurable {
    private static final CoreMessageLogger LOG = Logger.getMessageLogger(
        CoreMessageLogger.class,
        PrefixedEntitySequenceIdGenerator.class.getName()
    );

    /**
     * Configures the name of the table to use.  The default value is {@link #DEF_TABLE}
     */
    public static final String TABLE_PARAM = "table_name";

    /**
     * The default {@link #TABLE_PARAM} value
     */
    public static final String DEF_TABLE = "hibernate_sequences";

    /**
     * The name of column which holds the sequence value.  The default value is {@link #DEF_VALUE_COLUMN}
     */
    public static final String VALUE_COLUMN_PARAM = "value_column_name";

    /**
     * The default {@link #VALUE_COLUMN_PARAM} value
     */
    public static final String DEF_VALUE_COLUMN = "next_val";

    /**
     * The name of the column which holds the segment key.  The segment defines the different buckets (segments)
     * of values currently tracked in the table.  The default value is {@link #DEF_SEGMENT_COLUMN}
     */
    public static final String SEGMENT_COLUMN_PARAM = "segment_column_name";

    /**
     * The default {@link #SEGMENT_COLUMN_PARAM} value
     */
    public static final String DEF_SEGMENT_COLUMN = "sequence_name";

    /**
     * Indicates the length of the column defined by {@link #SEGMENT_COLUMN_PARAM}.  Used in schema export.  The
     * default value is {@link #DEF_SEGMENT_LENGTH}
     */
    public static final String SEGMENT_LENGTH_PARAM = "segment_value_length";

    /**
     * The default {@link #SEGMENT_LENGTH_PARAM} value
     */
    public static final int DEF_SEGMENT_LENGTH = 255;

    /**
     * Indicates the initial value to use.  The default value is {@link #DEFAULT_INITIAL_VALUE}
     */
    public static final String INITIAL_PARAM = "initial_value";

    /**
     * The default {@link #INITIAL_PARAM} value
     */
    public static final int DEFAULT_INITIAL_VALUE = 1;

    /**
     * Indicates the increment size to use.  The default value is {@link #DEFAULT_INCREMENT_SIZE}
     */
    public static final String INCREMENT_PARAM = "increment_size";

    /**
     * The default {@link #INCREMENT_PARAM} value
     */
    public static final int DEFAULT_INCREMENT_SIZE = 1;

    /**
     * Indicates the optimizer to use, either naming a {@link Optimizer} implementation class or by naming
     * a {@link StandardOptimizerDescriptor} by name
     */
    public static final String OPT_PARAM = "optimizer";

    public static final String NUMBER_FORMAT_PARAM = "numberFormat";
    public static final String NUMBER_FORMAT_DEFAULT = "%05d";

    private String sequenceValueFormat;

    private QualifiedName qualifiedTableName;
    private String renderedTableName;

    private String segmentColumnName;
    private int segmentValueLength;

    private String valueColumnName;
    private int initialValue;
    private int incrementSize;

    private String selectQuery;
    private String insertQuery;
    private String updateQuery;

    private Optimizer optimizer;
    private long accessCount;

    private String contributor;

    /**
     * The name of the table in which we store this generator's persistent state.
     *
     * @return The table name.
     */
    public final String getTableName() {
        return qualifiedTableName.render();
    }

    /**
     * The name of the column in which we store the segment to which each row
     * belongs.  The value here acts as PK.
     *
     * @return The segment column name
     */
    public final String getSegmentColumnName() {
        return segmentColumnName;
    }


    /**
     * The size of the {@link #getSegmentColumnName segment column} in the
     * underlying table.
     * <p/>
     * <b>NOTE</b> : should really have been called 'segmentColumnLength' or
     * even better 'segmentColumnSize'
     *
     * @return the column size.
     */
    @SuppressWarnings("UnusedDeclaration")
    public final int getSegmentValueLength() {
        return segmentValueLength;
    }

    /**
     * The name of the column in which we store our persistent generator value.
     *
     * @return The name of the value column.
     */
    public final String getValueColumnName() {
        return valueColumnName;
    }

    /**
     * The initial value to use when we find no previous state in the
     * generator table corresponding to our sequence.
     *
     * @return The initial value to use.
     */
    public final int getInitialValue() {
        return initialValue;
    }

    /**
     * The amount of increment to use.  The exact implications of this
     * depends on the {@link #getOptimizer() optimizer} being used.
     *
     * @return The increment amount.
     */
    public final int getIncrementSize() {
        return incrementSize;
    }

    /**
     * The optimizer being used by this generator.
     *
     * @return Out optimizer.
     */
    public final Optimizer getOptimizer() {
        return optimizer;
    }

    /**
     * Getter for property 'tableAccessCount'.  Only really useful for unit test
     * assertions.
     *
     * @return Value for property 'tableAccessCount'.
     */
    public final long getTableAccessCount() {
        return accessCount;
    }

    @Override
    public void configure(Type type, Properties params, ServiceRegistry serviceRegistry) throws MappingException {

        String numberFormat = ConfigurationHelper.getString(NUMBER_FORMAT_PARAM, params, NUMBER_FORMAT_DEFAULT).replace("%", "%2$");

        this.sequenceValueFormat = "%1$s-" + numberFormat;

        final JdbcEnvironment jdbcEnvironment = serviceRegistry.getService( JdbcEnvironment.class );

        qualifiedTableName = determineGeneratorTableName( params, jdbcEnvironment );
        segmentColumnName = determineSegmentColumnName( params, jdbcEnvironment );
        valueColumnName = determineValueColumnName( params, jdbcEnvironment );
        segmentValueLength = determineSegmentColumnSize( params );
        initialValue = determineInitialValue( params );
        incrementSize = determineIncrementSize( params );

        final String optimizationStrategy = ConfigurationHelper.getString(
            OPT_PARAM,
            params,
            OptimizerFactory.determineImplicitOptimizerName( incrementSize, params )
        );

//        final BasicTypeRegistry basicTypeRegistry = database.getTypeConfiguration().getBasicTypeRegistry();

        optimizer = OptimizerFactory.buildOptimizer(
            optimizationStrategy,
//            basicTypeRegistry.resolve( StandardBasicTypes.LONG ).getReturnedClass(),
            Long.class, //ignore provided type as it will be based on a String field and we want to generate Longs.
            incrementSize,
            ConfigurationHelper.getInt( INITIAL_PARAM, params, -1 )
        );

        contributor = params.getProperty( CONTRIBUTOR_NAME );
        if ( contributor == null ) {
            contributor = "orm";
        }
    }

    /**
     * Determine the table name to use for the generator values.
     * <p/>
     * Called during {@link #configure configuration}.
     *
     * @see #getTableName()
     * @param params The params supplied in the generator config (plus some standard useful extras).
     * @param jdbcEnvironment The JDBC environment
     * @return The table name to use.
     */
    @SuppressWarnings("UnusedParameters")
    protected QualifiedName determineGeneratorTableName(Properties params, JdbcEnvironment jdbcEnvironment) {
        final String tableName = ConfigurationHelper.getString( TABLE_PARAM, params, DEF_TABLE );

        if ( tableName.contains( "." ) ) {
            return QualifiedNameParser.INSTANCE.parse( tableName );
        }
        else {
            // todo : need to incorporate implicit catalog and schema names
            final Identifier catalog = jdbcEnvironment.getIdentifierHelper().toIdentifier(
                ConfigurationHelper.getString( CATALOG, params )
            );
            final Identifier schema = jdbcEnvironment.getIdentifierHelper().toIdentifier(
                ConfigurationHelper.getString( SCHEMA, params )
            );
            return new QualifiedNameParser.NameParts(
                catalog,
                schema,
                jdbcEnvironment.getIdentifierHelper().toIdentifier( tableName )
            );
        }
    }

    /**
     * Determine the name of the column used to indicate the segment for each
     * row.  This column acts as the primary key.
     * <p/>
     * Called during {@link #configure configuration}.
     *
     * @see #getSegmentColumnName()
     * @param params The params supplied in the generator config (plus some standard useful extras).
     * @param jdbcEnvironment The JDBC environment
     * @return The name of the segment column
     */
    @SuppressWarnings("UnusedParameters")
    protected String determineSegmentColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
        final String name = ConfigurationHelper.getString( SEGMENT_COLUMN_PARAM, params, DEF_SEGMENT_COLUMN );
        return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
    }

    /**
     * Determine the name of the column in which we will store the generator persistent value.
     * <p/>
     * Called during {@link #configure configuration}.
     *
     * @see #getValueColumnName()
     * @param params The params supplied in the generator config (plus some standard useful extras).
     * @param jdbcEnvironment The JDBC environment
     * @return The name of the value column
     */
    @SuppressWarnings("UnusedParameters")
    protected String determineValueColumnName(Properties params, JdbcEnvironment jdbcEnvironment) {
        final String name = ConfigurationHelper.getString( VALUE_COLUMN_PARAM, params, DEF_VALUE_COLUMN );
        return jdbcEnvironment.getIdentifierHelper().toIdentifier( name ).render( jdbcEnvironment.getDialect() );
    }

    /**
     * Determine the size of the {@link #getSegmentColumnName segment column}
     * <p/>
     * Called during {@link #configure configuration}.
     *
     * @see #getSegmentValueLength()
     * @param params The params supplied in the generator config (plus some standard useful extras).
     * @return The size of the segment column
     */
    protected int determineSegmentColumnSize(Properties params) {
        return ConfigurationHelper.getInt( SEGMENT_LENGTH_PARAM, params, DEF_SEGMENT_LENGTH );
    }

    protected int determineInitialValue(Properties params) {
        return ConfigurationHelper.getInt( INITIAL_PARAM, params, DEFAULT_INITIAL_VALUE );
    }

    protected int determineIncrementSize(Properties params) {
        return ConfigurationHelper.getInt( INCREMENT_PARAM, params, DEFAULT_INCREMENT_SIZE );
    }

    @SuppressWarnings("unchecked")
    protected String buildSelectQuery(Dialect dialect) {
        final String alias = "tbl";
        final String query = "select " + StringHelper.qualify( alias, valueColumnName ) +
            " from " + renderedTableName + ' ' + alias +
            " where " + StringHelper.qualify( alias, segmentColumnName ) + "=?";
        final LockOptions lockOptions = new LockOptions( LockMode.PESSIMISTIC_WRITE );
        lockOptions.setAliasSpecificLockMode( alias, LockMode.PESSIMISTIC_WRITE );
        final Map updateTargetColumnsMap = Collections.singletonMap( alias, new String[] { valueColumnName } );
        return dialect.applyLocksToSql( query, lockOptions, updateTargetColumnsMap );
    }

    protected String buildUpdateQuery() {
        return "update " + renderedTableName +
            " set " + valueColumnName + "=? " +
            " where " + valueColumnName + "=? and " + segmentColumnName + "=?";
    }

    protected String buildInsertQuery() {
        return "insert into " + renderedTableName + " (" + segmentColumnName + ", " + valueColumnName + ") " + " values (?,?)";
    }

    private IntegralDataTypeHolder makeValue() {
        return IdentifierGeneratorHelper.getIntegralDataTypeHolder( Long.class );
    }

    @Override
    public Serializable generate(SharedSessionContractImplementor session,
                                 Object object) throws HibernateException {
        EntityGroupingIdentifier gid =(EntityGroupingIdentifier)object;
        String groupRef = gid.getEntityReferenceGroupPrefix();
        String segmentRef = gid.getEntityReferenceGroupDiscriminator()+groupRef;
        Serializable id = generateOriginal(session, object, segmentRef);
        return String.format(sequenceValueFormat,groupRef,id);
    }


    public Serializable generateOriginal(final SharedSessionContractImplementor session, final Object obj, String segmentValue) {
        final SqlStatementLogger statementLogger = session.getFactory().getServiceRegistry()
            .getService( JdbcServices.class )
            .getSqlStatementLogger();
        final SessionEventListenerManager statsCollector = session.getEventListenerManager();

        return optimizer.generate(
            new AccessCallback() {
                @Override
                public IntegralDataTypeHolder getNextValue() {
                    return session.getTransactionCoordinator().createIsolationDelegate().delegateWork(
                        new AbstractReturningWork<IntegralDataTypeHolder>() {
                            @Override
                            public IntegralDataTypeHolder execute(Connection connection) throws SQLException {
                                final IntegralDataTypeHolder value = makeValue();
                                int rows;
                                do {
                                    final PreparedStatement selectPS = prepareStatement( connection, selectQuery, statementLogger, statsCollector );

                                    try {
                                        selectPS.setString( 1, segmentValue );
                                        final ResultSet selectRS = executeQuery( selectPS, statsCollector );
                                        if ( !selectRS.next() ) {
                                            value.initialize( initialValue );

                                            final PreparedStatement insertPS = prepareStatement( connection, insertQuery, statementLogger, statsCollector );
                                            try {
                                                LOG.tracef( "binding parameter [%s] - [%s]", 1, segmentValue );
                                                insertPS.setString( 1, segmentValue );
                                                value.bind( insertPS, 2 );
                                                executeUpdate( insertPS, statsCollector );
                                            }
                                            finally {
                                                insertPS.close();
                                            }
                                        }
                                        else {
                                            value.initialize( selectRS, 1 );
                                        }
                                        selectRS.close();
                                    }
                                    catch (SQLException e) {
                                        LOG.unableToReadOrInitHiValue( e );
                                        throw e;
                                    }
                                    finally {
                                        selectPS.close();
                                    }


                                    final PreparedStatement updatePS = prepareStatement( connection, updateQuery, statementLogger, statsCollector );
                                    try {
                                        final IntegralDataTypeHolder updateValue = value.copy();
                                        if ( optimizer.applyIncrementSizeToSourceValues() ) {
                                            updateValue.add( incrementSize );
                                        }
                                        else {
                                            updateValue.increment();
                                        }
                                        updateValue.bind( updatePS, 1 );
                                        value.bind( updatePS, 2 );
                                        updatePS.setString( 3, segmentValue );
                                        rows = executeUpdate( updatePS, statsCollector );
                                    }
                                    catch (SQLException e) {
                                        LOG.unableToUpdateQueryHiValue( renderedTableName, e );
                                        throw e;
                                    }
                                    finally {
                                        updatePS.close();
                                    }
                                }
                                while ( rows == 0 );

                                accessCount++;

                                return value;
                            }
                        },
                        true
                    );
                }

                @Override
                public String getTenantIdentifier() {
                    return session.getTenantIdentifier();
                }
            }
        );
    }

    private PreparedStatement prepareStatement(
        Connection connection,
        String sql,
        SqlStatementLogger statementLogger,
        SessionEventListenerManager statsCollector) throws SQLException {
        statementLogger.logStatement( sql, FormatStyle.BASIC.getFormatter() );
        try {
            statsCollector.jdbcPrepareStatementStart();
            return connection.prepareStatement( sql );
        }
        finally {
            statsCollector.jdbcPrepareStatementEnd();
        }
    }

    private int executeUpdate(PreparedStatement ps, SessionEventListenerManager statsCollector) throws SQLException {
        try {
            statsCollector.jdbcExecuteStatementStart();
            return ps.executeUpdate();
        }
        finally {
            statsCollector.jdbcExecuteStatementEnd();
        }

    }

    private ResultSet executeQuery(PreparedStatement ps, SessionEventListenerManager statsCollector) throws SQLException {
        try {
            statsCollector.jdbcExecuteStatementStart();
            return ps.executeQuery();
        }
        finally {
            statsCollector.jdbcExecuteStatementEnd();
        }
    }

    @Override
    public void registerExportables(Database database) {
        final Dialect dialect = database.getJdbcEnvironment().getDialect();

        final Namespace namespace = database.locateNamespace(
            qualifiedTableName.getCatalogName(),
            qualifiedTableName.getSchemaName()
        );

        Table table = namespace.locateTable( qualifiedTableName.getObjectName() );
        if ( table == null ) {
            table = namespace.createTable(
                    qualifiedTableName.getObjectName(),
                    (identifier) -> new Table( contributor, namespace, identifier, false )
            );

            final BasicTypeRegistry basicTypeRegistry = database.getTypeConfiguration().getBasicTypeRegistry();

            // todo : note sure the best solution here.  do we add the columns if missing?  other?
            final Column segmentColumn = new ExportableColumn(
                database,
                table,
                segmentColumnName,
                basicTypeRegistry.resolve( StandardBasicTypes.STRING ),
                database.getTypeConfiguration()
                        .getDdlTypeRegistry()
                        .getTypeName( Types.VARCHAR, Size.length( segmentValueLength ) )
            );
            segmentColumn.setNullable( false );
            table.addColumn( segmentColumn );

            // lol
            table.setPrimaryKey( new PrimaryKey( table ) );
            table.getPrimaryKey().addColumn( segmentColumn );

            final Column valueColumn = new ExportableColumn(
                database,
                table,
                valueColumnName,
                basicTypeRegistry.resolve( StandardBasicTypes.LONG )
            );
            table.addColumn( valueColumn );
        }

        // allow physical naming strategies a chance to kick in
        this.renderedTableName = database.getJdbcEnvironment().getQualifiedObjectNameFormatter().format(
            table.getQualifiedTableName(),
            dialect
        );

        this.selectQuery = buildSelectQuery( dialect );
        this.updateQuery = buildUpdateQuery();
        this.insertQuery = buildInsertQuery();
    }
}