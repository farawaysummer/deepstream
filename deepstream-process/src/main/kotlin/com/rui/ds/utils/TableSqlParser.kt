package com.rui.ds.utils

import com.rui.ds.common.TableContext
import com.rui.ds.log.Logging
import com.rui.ds.log.logger
import org.apache.calcite.sql.*
import org.apache.calcite.sql.parser.SqlParser
import java.util.*
import java.util.stream.Collectors

object TableSqlParser: Logging {

    fun extractQueryTables(sql: String): List<TableContext> {
        logger().info("解析SQL:\n$sql")
        val result = SqlParser.create(sql).parseQuery()
        val tables = mutableSetOf<String>()
        extractTablesInSql(result, tables)

        val first = toTableContext(tables.first(), TableContext.TABLE_TYPE_CDC)
        val rest = tables.drop(1).map { toTableContext(it, TableContext.TABLE_TYPE_DIM) }.toTypedArray()

        return listOf(first, *rest)
    }

    fun extractColumns(sql: String): List<String> {
        val columns = mutableSetOf<String>()
        val result = SqlParser.create(sql).parseQuery()
        extractSelectColumnsInSql(result, columns)

        return columns.toList()
    }

    fun extractInsertTable(sql: String): TableContext {
        val result = SqlParser.create(sql).parseQuery()
        val tables = mutableSetOf<String>()
        extractTablesInSql(result, tables)

        return toTableContext(tables.first(), TableContext.TABLE_TYPE_DIM)
    }

    private fun toTableContext(fullName: String, type: String): TableContext {
        val parts = fullName.split(".")
        return if (parts.size == 1) {
            TableContext("", parts[0], type)
        } else if (parts.size >= 2) {
            TableContext(parts[0], parts[1], type)
        } else {
            TableContext("", fullName, type)
        }
    }

    private fun extractTablesInSql(sqlNode: SqlNode, tables: MutableSet<String>) {
        when (sqlNode.kind) {
            SqlKind.SELECT -> extractTablesInSqlSelect(sqlNode as SqlSelect, tables)
            SqlKind.INSERT -> extractTablesInSqlInsert(sqlNode as SqlInsert, tables)
            else -> throw RuntimeException("unknown sql types...")
        }
    }

    private fun extractTablesInSqlInsert(sqlInsert: SqlInsert, tables: MutableSet<String>) {
        extractSourceTableInSql(sqlInsert, false, tables)
        if (sqlInsert.targetTable is SqlIdentifier) {
            tables.add((sqlInsert.targetTable as SqlIdentifier).toString().uppercase(Locale.getDefault()))
        }
    }

    private fun extractTablesInSqlSelect(sqlSelect: SqlSelect, tables: MutableSet<String>) {
        extractSourceTableInSql(sqlSelect, false, tables)
    }

    private fun extractSourceTableInSql(sqlNode: SqlNode?, fromOrJoin: Boolean, tables: MutableSet<String>) {
        if (sqlNode == null) {
            return
        } else {
            when (sqlNode.kind) {
                SqlKind.SELECT -> {
                    val selectNode = sqlNode as SqlSelect
                    extractSourceTableInSql(selectNode.from, true, tables)
                    val selectList = selectNode.selectList.list.stream().filter { t: SqlNode -> t is SqlCall }
                        .collect(Collectors.toList())
                    for (select in selectList) {
                        extractSourceTableInSql(select, false, tables)
                    }
                    extractSourceTableInSql(selectNode.where, false, tables)
                    extractSourceTableInSql(selectNode.having, false, tables)
                }
                SqlKind.JOIN -> {
                    extractSourceTableInSql((sqlNode as SqlJoin).left, true, tables)
                    extractSourceTableInSql(sqlNode.right, true, tables)
                }
                SqlKind.AS -> if ((sqlNode as SqlCall).operandCount() >= 2) {
                    extractSourceTableInSql(sqlNode.operand(0), fromOrJoin, tables)
                }
                SqlKind.IDENTIFIER ->                     //
                    if (fromOrJoin) {
                        tables.add((sqlNode as SqlIdentifier).toString().uppercase(Locale.getDefault()))
                    }
                else -> if (sqlNode is SqlCall) {
                    for (node in sqlNode.operandList) {
                        extractSourceTableInSql(node, false, tables)
                    }
                }
            }
        }
    }

    private fun extractSelectColumnsInSql(sqlNode: SqlNode?, columns: MutableSet<String>) {
        if (sqlNode == null) {
            return
        }

        if (sqlNode is SqlSelect) {
            for (node in sqlNode.selectList) {
                extractSelectColumnsInSql(node, columns)
            }
        } else if (sqlNode is SqlCall) {
            if (sqlNode.kind == SqlKind.AS) {
                val asName = sqlNode.operand<SqlNode>(1)
                columns.add(asName.toString())
            } else if (sqlNode.kind == SqlKind.SELECT) {
                extractSelectColumnsInSql(sqlNode, columns)
            }
        } else if (sqlNode is SqlIdentifier) {
            columns.add(sqlNode.toString())
        }
    }
}