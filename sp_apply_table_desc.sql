CREATE OR REPLACE PROCEDURE SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.SP_APPLY_TABLE_DESC(
    "DB_NAME" VARCHAR, 
    "SCHEMA_NAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    var getAuditSQL = `
        SELECT TABLE_NAME, TABLE_DESC, COLUMN_DESC
        FROM SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_DETAILS
        WHERE DATABASE_NAME = ? 
          AND SCHEMA_NAME = ? 
          AND STATUS = ''SUCCESS''
    `;

    var stmt = snowflake.createStatement({ sqlText: getAuditSQL, binds: [DB_NAME, SCHEMA_NAME] });
    var rs = stmt.execute();
    var results = [];

    while (rs.next()) {
        var tableName = rs.getColumnValue(1);
        var tableDesc = rs.getColumnValue(2);
        var columnDescVariant = rs.getColumnValue(3);
        var columnDescJSON = null;

        // Convert VARIANT to JSON text
        if (columnDescVariant !== null) {
            try {
                // stringify and parse because getColumnValue returns VARIANT as JS object
                columnDescJSON = (typeof columnDescVariant === ''string'') 
                    ? JSON.parse(columnDescVariant)
                    : columnDescVariant;
            } catch (err) {
                columnDescJSON = {};
            }
        }

        var fullTableName = `"${DB_NAME}"."${SCHEMA_NAME}"."${tableName}"`;

        // 1️⃣ Apply table-level comment
        if (tableDesc) {
            try {
                var commentTableSQL = `COMMENT ON TABLE ${fullTableName} IS ?`;
                snowflake.createStatement({ sqlText: commentTableSQL, binds: [tableDesc] }).execute();

                // Log success
                snowflake.createStatement({
                    sqlText: `
                        INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_APPLY_AUDIT
                        (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DESCRIPTION, ACTION_TYPE, STATUS, ERROR_MESSAGE)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    `,
                    binds: [DB_NAME, SCHEMA_NAME, tableName, null, tableDesc, ''TABLE_DESC'', ''SUCCESS'', null]
                }).execute();

                results.push(`✅ Table comment applied: ${fullTableName}`);
            } catch (err) {
                var errorMsg = err.toString().substring(0, 500);
                snowflake.createStatement({
                    sqlText: `
                        INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_APPLY_AUDIT
                        (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DESCRIPTION, ACTION_TYPE, STATUS, ERROR_MESSAGE)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    `,
                    binds: [DB_NAME, SCHEMA_NAME, tableName, null, tableDesc, ''TABLE_DESC'', ''ERROR'', errorMsg]
                }).execute();

                results.push(`❌ Error applying table comment: ${fullTableName} - ${errorMsg}`);
            }
        }

        // 2️⃣ Apply column-level comments
        if (columnDescJSON && columnDescJSON.COLUMNS && Array.isArray(columnDescJSON.COLUMNS)) {
            for (var i = 0; i < columnDescJSON.COLUMNS.length; i++) {
                var col = columnDescJSON.COLUMNS[i];
                if (col.name && col.description) {
                    var fullColumnName = `"${DB_NAME}"."${SCHEMA_NAME}"."${tableName}"."${col.name}"`;
                    try {
                        // Use COMMENT ON COLUMN syntax
                        var commentColumnSQL = `COMMENT ON COLUMN ${fullColumnName} IS ?`;
                        var stmtComment = snowflake.createStatement({ sqlText: commentColumnSQL, binds: [col.description] });
                        stmtComment.execute();

                        // Log success
                        snowflake.createStatement({
                            sqlText: `
                                INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_APPLY_AUDIT
                                (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DESCRIPTION, ACTION_TYPE, STATUS, ERROR_MESSAGE)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            `,
                            binds: [DB_NAME, SCHEMA_NAME, tableName, col.name, col.description, ''COLUMN_DESC'', ''SUCCESS'', null]
                        }).execute();

                        results.push(`✅ Column comment applied: ${fullColumnName}`);
                    } catch (err) {
                        var errorMsg = err.toString().substring(0, 500);
                        snowflake.createStatement({
                            sqlText: `
                                INSERT INTO SNOWFLAKE_LEARNING_DB.DATA_GOVERNANACE.T_DESCRIPTION_APPLY_AUDIT
                                (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, DESCRIPTION, ACTION_TYPE, STATUS, ERROR_MESSAGE)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            `,
                            binds: [DB_NAME, SCHEMA_NAME, tableName, col.name, col.description, ''COLUMN_DESC'', ''ERROR'', errorMsg]
                        }).execute();

                        results.push(`❌ Error applying column comment: ${fullColumnName} - ${errorMsg}`);
                    }
                }
            }
        }
    }

    return results.join(''\\\\n'');
} catch (err) {
    return ''Unexpected failure: '' + err.toString();
}
';
