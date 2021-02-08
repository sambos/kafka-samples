package rsol.example.kafka.producer.util;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.apache.tomcat.util.ExceptionUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JdbcBatchInsertTest {

	public static void main(String[] args) throws Exception {

		List<String> tables = new ArrayList<String>();
		tables.add("TABLE_NAME");
		Map<String, Object> tableInfo = getMetadata(tables, "schema");
		validateMetaData(tables, tableInfo);

	}
	
	public static boolean isNumeric(String str) { 
		  try {  
		    Double.parseDouble(str);  
		    return true;
		  } catch(NumberFormatException e){  
		    return false;  
		  }  
		}

	private static void validateMetaData(List<String> tableNames, Map<String, Object> tablesInfo) {
		
		for (String table : tableNames) {
			Map<String, Object> tinfo = (Map<String, Object>) tablesInfo.get(table);
			int c = ((Map<String, Object>) tinfo.get("COLUMNS")).size();
			int pk = ((Map<String, Object>) tinfo.get("PRIMARY_KEYS")).size();
			System.out.println(String.format("meta-data for table:%s : columns:[%d], pks:[%d]", table, c, pk));
		}
	}
	
	public static void executeUpdate(String table, String schema, List<Map<String, Object>> records, Map<String, Object> table_meta) throws Exception {
		String [] pks = ((Map<String, Object>) table_meta.get("PRIMARY_KEYS")).keySet().toArray(new String[0]);
		
		String[] columns = records.iterator().next().keySet().toArray(new String[0]);
		String sql = generateUpdateSql(table, columns, pks );
		System.out.println(sql);
		sql = String.format(sql, schema);
		
		saveBatch(sql, records, appendArrays(columns, pks), (Map<String, Object>) table_meta.get("COLUMNS"));
	}
	
	public static void executeInsert(String table, String schema, List<Map<String, Object>> records, Map<String, Object> table_meta) throws Exception {
		
		String[] columns = records.iterator().next().keySet().toArray(new String[0]);
		String sql = generateInsertSql(table, columns );
		System.out.println(sql);
		
		sql = String.format(sql, schema);

		saveBatch(sql, records, columns, (Map<String, Object>) table_meta.get("COLUMNS"));
	}
	
	private static String[] appendArrays(String[] first, String[] second) {
		String[] both = Arrays.copyOf(first, first.length + second.length);
		System.arraycopy(second, 0, both, first.length, second.length);	
		return both;		
	}
	
	public static void saveBatch(String sql, List<Map<String, Object>> records, String[] columns, Map<String, Object> metaData) throws Exception {

		JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());

		jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				Map<String, Object> map = records.get(i);
			
				int paramIndex = 1;
				for(int colIndex=0; colIndex < columns.length; colIndex++) {
					Object value = map.get(columns[colIndex]);
					String sqlTypeName = (String) metaData.get(columns[colIndex]);
					
					//System.out.println(colIndex + " column:" + columns[colIndex] +" : value:"+value + "  type:" + sqlTypeName);

					if(value == null) {
						ps.setNull(paramIndex++, Types.NULL);
					}else {
						setStatementValue(ps, paramIndex++, value, sqlTypeName);
					}
				}
			}
			
			@Override
			public int getBatchSize() {
				return records.size();
			}
		});
	}
	

	public static void setStatementValue(PreparedStatement ps, int index, Object value, String sqlTypeName) throws SQLException {
		
		if (sqlTypeName.equals("CHAR") || sqlTypeName.equals("NCHAR") 
				|| sqlTypeName.equals("VARCHAR") || sqlTypeName.equals("VARCHAR2")
				|| sqlTypeName.equals("NVARCHAR2")) {
			ps.setString(index, value.toString());
		}
		else if (sqlTypeName.equals("NUMBER")) {
			ps.setBigDecimal(index, new BigDecimal(""+value));
		}else if (sqlTypeName.equals("DATE") || sqlTypeName.equals("TIME") || sqlTypeName.equals("TIMESTAMP")){
			ps.setTimestamp(index, parseDate((String)value, "yyyy-MM-dd"));
		}else {
			// try to infer the value type or set as object 
			ps.setObject(index, value);
		}
	}
	
	public static java.sql.Timestamp parseDate(String value, String pattern) {
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		
		try {
			java.util.Date myDate = format.parse(value);

			return new java.sql.Timestamp(myDate.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
		

	public static List<Map<String, Object>> buildMessage() throws Exception {
		
		List<Map<String, Object>> messages = new ArrayList<Map<String, Object>>();
		
		return messages;
	}
	
	//name, index, type, value
	//names[] values[], types[]
	
	public static String generateInsertSql(String table, String[] columns) {

		StringBuilder sqlString = new StringBuilder("INSERT INTO ").append("%s.").append(table);
		
		StringBuilder columnString = new StringBuilder(" (").append(String.join(", ", columns)).append(") ");

		sqlString.append(columnString);
		
		sqlString.append("VALUES (");
		for (int m = 0; m < columns.length; m++) {
			if (m > 0) {
				sqlString.append(", ");
			}
			sqlString.append('?');
		}
		sqlString.append(')');
	
		return sqlString.toString();
	}
	
	public static String generateUpdateSql(String table, String[] columns, String[] pks) {

		List<String> list = new ArrayList<String>();
		for(String column : columns)
		  list.add(column + " = ?");
		StringBuilder columnString = new StringBuilder(" ").append(String.join(", ", list));
		
		// pk
		list = new ArrayList<String>();
		for(String key : pks)
		  list.add(key + " = ?");
		StringBuilder keyString = new StringBuilder(" ").append(String.join(" AND ", list));
		
		StringBuilder updateSql = new StringBuilder("UPDATE ").append("%s.").append(table).append(" SET").append(columnString).append(" WHERE").append(keyString);
		return updateSql.toString();
	}

	public static void batchUpdate() {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(getDataSource());
		String[] sqls = {"",""};
		jdbcTemplate.batchUpdate(sqls);
	}
	
	
	public static Boolean checkKey(String table, String prefix) {
		if (table.startsWith(prefix))
			return true;
		return false;
	}
	
	public static List<String> getTables(String prefix, String defaultSchema){
		
		
		List<String> tables = new ArrayList<>();
		
		try (Connection con = getDataSource().getConnection()) {
			DatabaseMetaData databaseMetaData = con.getMetaData();
							
				Map<String, Object> response = new HashMap<>();				
				
				try (ResultSet rs = databaseMetaData.getTables(null, defaultSchema, null, new String[] { "TABLE" })) {
					while (rs.next()) {
						if(checkKey(rs.getString("TABLE_NAME"), prefix))
						     tables.add(rs.getString("TABLE_NAME"));
					}
				} catch (Exception e) {
					//log.info("Failed to fetch column info for table " + table, e.getMessage(), e);
				}

		} catch (Exception e) {
			//log.info("Error occurred while processing!", e.getMessage(), e);
			//throw e;
			e.printStackTrace();
		}
		return tables;
	}
	
	public static Map<String, Object> getMetadata(List<String> tables, String defaultSchema) {
		
		Map<String, Object> tableInfo = new HashMap<>();
		
		try (Connection con = getDataSource().getConnection()) {
			DatabaseMetaData databaseMetaData = con.getMetaData();
//			System.out.println("Supports Batch Updates :" + databaseMetaData.supportsBatchUpdates());
//			System.out.println(databaseMetaData.getDatabaseProductName());
//			System.out.println(databaseMetaData.getDatabaseProductVersion());
//			System.out.println(databaseMetaData.getDriverVersion());
			Map<String, String> columns = null;
			Map<String, String> primaryKeys = null;
			
			ResultSet resultSet = null;
			for (String table : tables) {
				String tableName = table.replace("DLRMERGED", "");
				
				Map<String, Object> response = new HashMap<>();				
				columns = new TreeMap<>();
				primaryKeys = new TreeMap<>();
				
				try (ResultSet rs = databaseMetaData.getColumns(null, defaultSchema, tableName, null)) {
					while (rs.next()) {
						columns.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME")); // TYPE_NAME, DATA_TYPE,
																								// IS_NULLABLE
					}
				} catch (Exception e) {
					//log.info("Failed to fetch column info for table " + table, e.getMessage(), e);
				}

				try (ResultSet rs = databaseMetaData.getPrimaryKeys(null, defaultSchema, tableName)) {
					while (rs.next()) {
						primaryKeys.put(rs.getString("COLUMN_NAME"), columns.get(rs.getString("COLUMN_NAME")));
					}
				} catch (Exception e) {
					//log.info("Failed to fetch primary key info for table " + table, e.getMessage(), e);
				}
				
				response.put("COLUMNS", columns);
				response.put("PRIMARY_KEYS", primaryKeys);
				tableInfo.put(table, response);

			}
		} catch (Exception e) {
			//log.info("Error occurred while processing!", e.getMessage(), e);
			//throw e;
			e.printStackTrace();
		}
		return tableInfo;
	}
	  public static DataSource getDataSource() { DriverManagerDataSource dataSource
				= new DriverManagerDataSource();
		dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
		dataSource.setUrl("jdbc:oracle:thin:@server:1521:user");
		dataSource.setUsername("user");
		dataSource.setPassword("pwd");
    
		return dataSource;
	}
	  
	  
}
