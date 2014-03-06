package org.apache.jmeter.protocol.jdbc.sampler;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class JavaJDBCSampler extends AbstractJavaSamplerClient {


    private String userName;

    public static final String DEFAULT_USER_NAME = "admin";

    private static final String USER_NAME = "User Name";
    
    private String userPassword;

    public static final String DEFAULT_USER_PASSWORD = "admin";

    private static final String USER_PASSWORD = "User Password";
    
    private String serverURL;

    public static final String DEFAULT_SERVER_URL = "jdbc://";

    private static final String SERVER_URL = "Server URL";
    
    private String driverClass;

    public static final String DEFAULT_DRIVER_CLASS = "com.";

    private static final String DRIVER_CLASS = "Driver Class";
    
    private String query;

    public static final String DEFAULT_QUERY = "SELECT 1";

    private static final String QUERY = "Query";
    
    private long rowLimit;

    public static final long DEFAULT_ROW_LIMIT = 0;

    private static final String ROW_LIMIT = "Row Limit";    
    
    
    

    @Override
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument(USER_NAME, DEFAULT_USER_NAME);
        params.addArgument(USER_PASSWORD, DEFAULT_USER_PASSWORD);
        params.addArgument(SERVER_URL, DEFAULT_SERVER_URL);
        params.addArgument(DRIVER_CLASS, DEFAULT_DRIVER_CLASS);
        params.addArgument(QUERY, DEFAULT_QUERY);
        params.addArgument(ROW_LIMIT, Long.toString(DEFAULT_ROW_LIMIT));
        return params;
    }

    private void setupValues(JavaSamplerContext context) {

        userName = context.getParameter(USER_NAME, DEFAULT_USER_NAME);
        userPassword = context.getParameter(USER_PASSWORD, DEFAULT_USER_PASSWORD);
        serverURL = context.getParameter(SERVER_URL, DEFAULT_SERVER_URL);
        driverClass = context.getParameter(DRIVER_CLASS, DEFAULT_DRIVER_CLASS);
        query = context.getParameter(QUERY, DEFAULT_QUERY);
        rowLimit = context.getLongParameter(ROW_LIMIT, DEFAULT_ROW_LIMIT);
    }

    /**
     * Perform a single sample.<br>
     * This method returns a <code>SampleResult</code> object.
     * 
     * <pre>
     * The following fields are always set:
     * - responseCode (default "")
     * - responseMessage (default "")
     * - label (default "JavaTest")
     * - success (default true)
     * </pre>
     * 
     * The following fields are set from the user-defined parameters, if
     * supplied:
     * 
     * <pre>
     * -samplerData - responseData
     * </pre>
     * 
     * @see org.apache.jmeter.samplers.SampleResult#sampleStart()
     * @see org.apache.jmeter.samplers.SampleResult#sampleEnd()
     * @see org.apache.jmeter.samplers.SampleResult#setSuccessful(boolean)
     * @see org.apache.jmeter.samplers.SampleResult#setSampleLabel(String)
     * @see org.apache.jmeter.samplers.SampleResult#setResponseCode(String)
     * @see org.apache.jmeter.samplers.SampleResult#setResponseMessage(String)
     * @see org.apache.jmeter.samplers.SampleResult#setResponseData(byte [])
     * @see org.apache.jmeter.samplers.SampleResult#setDataType(String)
     * 
     * @param context
     *            the context to run with. This provides access to
     *            initialization parameters.
     * 
     * @return a SampleResult giving the results of this sample.
     */
    public SampleResult runTest(JavaSamplerContext context) {
        setupValues(context);

        SampleResult results = new SampleResult();

        results.setDataType(SampleResult.TEXT);

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        // Record sample start time.
        results.sampleStart();
        
        try {
            
            Class.forName(driverClass).newInstance();
            conn = DriverManager.getConnection(serverURL, userName,
                    userPassword);
            
            st = conn.createStatement();
            rs = st.executeQuery(query);
            String result = getStringFromResultSet(rs);
            results.setResponseData(result.getBytes());
            
            results.setSuccessful(true);
        } catch (Exception e) {
            getLogger().error("JavaJDBCSampler: error during sample: ", e);
            results.setSuccessful(false);
        } finally {
            // Record end time.
            results.sampleEnd();
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                getLogger().error("JavaJDBCSampler: error during sample: ", e);
                results.setSuccessful(false);
            }
        }

        return results;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        //
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        //
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

        final int limit = (int) (rowLimit > 0 ? rowLimit : Integer.MAX_VALUE);
        int idx = 0;
        while (idx < limit && rs.next()) {
            idx++;
            for (int i = 1; i <= numColumns; i++) {
                Object o = rs.getObject(i);
                if (o instanceof byte[]) {
                    o = new String((byte[]) o, "UTF-8");
                }
                sb.append(o);
                if (i==numColumns){
                    sb.append('\n');
                } else {
                    sb.append('\t');
                }
            }
        }

        return sb.toString();
    }
    

}
