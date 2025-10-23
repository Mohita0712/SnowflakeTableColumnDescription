CREATE OR REPLACE PROCEDURE SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.SP_GENERATE_TABLE_DESC_FOR_SCHEMA(
    "DB_NAME" VARCHAR, 
    "SCHEMA_NAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  var getTablesSQL = `SELECT table_name
                      FROM ${DB_NAME}.information_schema.tables
                      WHERE table_schema = ''${SCHEMA_NAME}''
                        AND table_type = ''BASE TABLE''`;

  var stmt = snowflake.createStatement({ sqlText: getTablesSQL });
  var rs = stmt.execute();
  var results = [];

  while (rs.next()) {
    var tableName = rs.getColumnValue(1);
    var fullTableName = `${DB_NAME}.${SCHEMA_NAME}.${tableName}`;

    var callSQL = `CALL AI_GENERATE_TABLE_DESC(
                      ''${fullTableName}'',
                      { ''describe_columns'': true, ''use_table_data'': true }
                   );`;

    try {
      var callStmt = snowflake.createStatement({ sqlText: callSQL });
      var callRs = callStmt.execute();

      var aiResultStr = null;
      var tableDesc = null;

      if (callRs.next()) {
        var aiResult = callRs.getColumnValue(1); // may be variant/object or string or null

        if (aiResult !== null) {
          if (typeof aiResult === ''string'') {
            try {
              var parsed = JSON.parse(aiResult);

              // Extract table description
              if (parsed.TABLE && parsed.TABLE.length > 0 && parsed.TABLE[0].description) {
                tableDesc = parsed.TABLE[0].description;
              }

              // Keep only COLUMNS array for COLUMN_DESC
              if (parsed.COLUMNS) {
                aiResultStr = JSON.stringify({ COLUMNS: parsed.COLUMNS });
              } else {
                aiResultStr = JSON.stringify({ COLUMNS: [] });
              }

            } catch (e) {
              // fallback if parse fails
              aiResultStr = null;
              tableDesc = null;
            }

          } else {
            // JS object returned
            if (aiResult.TABLE && aiResult.TABLE.length > 0 && aiResult.TABLE[0].description) {
              tableDesc = aiResult.TABLE[0].description;
            }

            if (aiResult.COLUMNS) {
              aiResultStr = JSON.stringify({ COLUMNS: aiResult.COLUMNS });
            } else {
              aiResultStr = JSON.stringify({ COLUMNS: [] });
            }
          }
        }
      }

      // Insert result
      if (aiResultStr !== null) {
        var insertSQL = `
          INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_DETAILS
            (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, STATUS, "TABLE_DESC", COLUMN_DESC, ERROR_MESSAGE)
          SELECT ?, ?, ?, ?, ?, PARSE_JSON(?), ?
        `;
        var insertStmt = snowflake.createStatement({
          sqlText: insertSQL,
          binds: [DB_NAME, SCHEMA_NAME, tableName, ''SUCCESS'', tableDesc, aiResultStr, null]
        });
        insertStmt.execute();
      } else {
        var insertSQL = `
          INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_DETAILS
            (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, STATUS, "TABLE_DESC", COLUMN_DESC, ERROR_MESSAGE)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `;
        var insertStmt = snowflake.createStatement({
          sqlText: insertSQL,
          binds: [DB_NAME, SCHEMA_NAME, tableName, ''SUCCESS'', tableDesc, null, null]
        });
        insertStmt.execute();
      }

      results.push(`Success: ${fullTableName}`);

    } catch (err) {
      var insertErrSQL = `
        INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_DETAILS
          (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, STATUS, "TABLE_DESC", COLUMN_DESC, ERROR_MESSAGE)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `;
      var insertErrStmt = snowflake.createStatement({
        sqlText: insertErrSQL,
        binds: [DB_NAME, SCHEMA_NAME, tableName, ''ERROR'', null, null, err.toString()]
      });
      insertErrStmt.execute();

      results.push(`Error on ${fullTableName}: ${err.toString()}`);
    }
  }

  return results.join(''\\n'');
';
