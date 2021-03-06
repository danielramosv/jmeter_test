package org.apache.jmeter.protocol.jdbc.sampler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jmeter.protocol.jdbc.AbstractJDBCTestElement;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

public class JDBCFetchlessSampler extends JDBCSampler {
	private static final long serialVersionUID = -659610836561775584L;

	private static final Logger log = LoggingManager.getLoggerForClass();

	private static final String COMMA = ","; // $NON-NLS-1$
	private static final char COMMA_CHAR = ',';

	// String used to indicate a null value
	private static final String NULL_MARKER = JMeterUtils.getPropDefault(
			"jdbcsampler.nullmarker", "]NULL["); // $NON-NLS-1$

	private static final String INOUT = "INOUT"; // $NON-NLS-1$

	private static final String OUT = "OUT"; // $NON-NLS-1$

	// key: name (lowercase) from java.sql.Types; entry: corresponding int value
	private static final Map<String, Integer> mapJdbcNameToInt;
	// read-only after class init

	static {
		// based on e291. Getting the Name of a JDBC Type from javaalmanac.com
		// http://javaalmanac.com/egs/java.sql/JdbcInt2Str.html
		mapJdbcNameToInt = new HashMap<String, Integer>();

		// Get all fields in java.sql.Types and store the corresponding int
		// values
		Field[] fields = java.sql.Types.class.getFields();
		for (int i = 0; i < fields.length; i++) {
			try {
				String name = fields[i].getName();
				Integer value = (Integer) fields[i].get(null);
				mapJdbcNameToInt.put(
						name.toLowerCase(java.util.Locale.ENGLISH), value);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e); // should not happen
			}
		}
	}

	// Query types (used to communicate with GUI)
	// N.B. These must not be changed, as they are used in the JMX files
	public static final String SELECT = "Select Statement"; // $NON-NLS-1$
	public static final String UPDATE = "Update Statement"; // $NON-NLS-1$
	public static final String CALLABLE = "Callable Statement"; // $NON-NLS-1$
	public static final String PREPARED_SELECT = "Prepared Select Statement"; // $NON-NLS-1$
	public static final String PREPARED_UPDATE = "Prepared Update Statement"; // $NON-NLS-1$
	public static final String COMMIT = "Commit"; // $NON-NLS-1$
	public static final String ROLLBACK = "Rollback"; // $NON-NLS-1$
	public static final String AUTOCOMMIT_FALSE = "AutoCommit(false)"; // $NON-NLS-1$
	public static final String AUTOCOMMIT_TRUE = "AutoCommit(true)"; // $NON-NLS-1$

	/**
	 * Cache of PreparedStatements stored in a per-connection basis. Each entry
	 * of this cache is another Map mapping the statement string to the actual
	 * PreparedStatement. At one time a Connection is only held by one thread
	 */
	private static final Map<Connection, Map<String, PreparedStatement>> perConnCache = new ConcurrentHashMap<Connection, Map<String, PreparedStatement>>();

	/**
	 * Creates a No-Fetch JDBCSampler.
	 */
	public JDBCFetchlessSampler() {

	}

	@Override
	protected byte[] execute(Connection conn) throws SQLException,
			UnsupportedEncodingException, IOException,
			UnsupportedOperationException {
		log.debug("executing jdbc");
		Statement stmt = null;

		try {
			// Based on query return value, get results
			String _queryType = getQueryType();
			if (SELECT.equals(_queryType)) {
				stmt = conn.createStatement();
				ResultSet rs = null;
				try {
					rs = stmt.executeQuery(getQuery());
					return getStringFromResultSet(rs).getBytes(ENCODING);
				} finally {
					close(rs);
				}
			} else if (CALLABLE.equals(_queryType)) {
				return super.execute(conn);
			} else if (UPDATE.equals(_queryType)) {
				return super.execute(conn);
			} else if (PREPARED_SELECT.equals(_queryType)) {
				PreparedStatement pstmt = getPreparedStatement(conn);
				setArguments(pstmt);
				ResultSet rs = null;
				try {
					rs = pstmt.executeQuery();
					return getStringFromResultSet(rs).getBytes(ENCODING);
				} finally {
					close(rs);
				}
			} else if (PREPARED_UPDATE.equals(_queryType)) {
				return super.execute(conn);
			} else if (ROLLBACK.equals(_queryType)) {
				return super.execute(conn);
			} else if (COMMIT.equals(_queryType)) {
				return super.execute(conn);
			} else if (AUTOCOMMIT_FALSE.equals(_queryType)) {
				return super.execute(conn);
			} else if (AUTOCOMMIT_TRUE.equals(_queryType)) {
				return super.execute(conn);
			} else { // User provided incorrect query type
				return super.execute(conn);
			}
		} finally {
			close(stmt);
		}
	}

	/**
	 * Gets a Data object from a ResultSet.
	 * 
	 * @param rs
	 *            ResultSet passed in from a database query
	 * @return a Data object
	 * @throws java.sql.SQLException
	 * @throws UnsupportedEncodingException
	 */
	private String getStringFromResultSet(ResultSet rs) throws SQLException,
			UnsupportedEncodingException {
		ResultSetMetaData meta = rs.getMetaData();

		StringBuilder sb = new StringBuilder();

		int numColumns = meta.getColumnCount();
		for (int i = 1; i <= numColumns; i++) {
			sb.append(meta.getColumnName(i));
			if (i == numColumns) {
				sb.append('\n');
			} else {
				sb.append('\t');
			}
		}

		return sb.toString();
	}

	/**
	 * Direct copy from {@link AbstractJDBCTestElement}.
	 */
	private PreparedStatement getPreparedStatement(Connection conn)
			throws SQLException {
		final boolean callable = false;
		Map<String, PreparedStatement> preparedStatementMap = perConnCache
				.get(conn);
		if (null == preparedStatementMap) {
			preparedStatementMap = new ConcurrentHashMap<String, PreparedStatement>();
			// As a connection is held by only one thread, we cannot already
			// have a
			// preparedStatementMap put by another thread
			perConnCache.put(conn, preparedStatementMap);
		}
		PreparedStatement pstmt = preparedStatementMap.get(getQuery());
		if (null == pstmt) {
			if (callable) {
				pstmt = conn.prepareCall(getQuery());
			} else {
				pstmt = conn.prepareStatement(getQuery());
			}
			// PreparedStatementMap is associated to one connection so
			// 2 threads cannot use the same PreparedStatement map at the same
			// time
			preparedStatementMap.put(getQuery(), pstmt);
		}
		pstmt.clearParameters();
		return pstmt;
	}

	/**
	 * Direct copy from {@link AbstractJDBCTestElement}.
	 */
	private int[] setArguments(PreparedStatement pstmt) throws SQLException,
			IOException {
		if (getQueryArguments().trim().length() == 0) {
			return new int[] {};
		}
		String[] arguments = CSVSaveService.csvSplitString(getQueryArguments(),
				COMMA_CHAR);
		String[] argumentsTypes = getQueryArgumentsTypes().split(COMMA);
		if (arguments.length != argumentsTypes.length) {
			throw new SQLException("number of arguments (" + arguments.length
					+ ") and number of types (" + argumentsTypes.length
					+ ") are not equal");
		}
		int[] outputs = new int[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			String argument = arguments[i];
			String argumentType = argumentsTypes[i];
			String[] arg = argumentType.split(" ");
			String inputOutput = "";
			if (arg.length > 1) {
				argumentType = arg[1];
				inputOutput = arg[0];
			}
			int targetSqlType = getJdbcType(argumentType);
			try {
				if (!OUT.equalsIgnoreCase(inputOutput)) {
					if (argument.equals(NULL_MARKER)) {
						pstmt.setNull(i + 1, targetSqlType);
					} else {
						pstmt.setObject(i + 1, argument, targetSqlType);
					}
				}
				if (OUT.equalsIgnoreCase(inputOutput)
						|| INOUT.equalsIgnoreCase(inputOutput)) {
					CallableStatement cs = (CallableStatement) pstmt;
					cs.registerOutParameter(i + 1, targetSqlType);
					outputs[i] = targetSqlType;
				} else {
					outputs[i] = java.sql.Types.NULL; // can't have an output
														// parameter type null
				}
			} catch (NullPointerException e) { // thrown by Derby JDBC (at
												// least) if there are no "?"
												// markers in statement
				throw new SQLException("Could not set argument no: " + (i + 1)
						+ " - missing parameter marker?");
			}
		}
		return outputs;
	}

	/**
	 * Direct copy from {@link AbstractJDBCTestElement}.
	 */
	private static int getJdbcType(String jdbcType) throws SQLException {
		Integer entry = mapJdbcNameToInt.get(jdbcType
				.toLowerCase(java.util.Locale.ENGLISH));
		if (entry == null) {
			try {
				entry = Integer.decode(jdbcType);
			} catch (NumberFormatException e) {
				throw new SQLException("Invalid data type: " + jdbcType);
			}
		}
		return (entry).intValue();
	}

}
