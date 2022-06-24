package com.rui.ds.facade.kettle.debug

import com.google.common.io.Files
import com.rui.ds.common.TableContext
import org.junit.Test
import java.io.File
import java.nio.charset.Charset
import kotlin.test.assertNotNull

class NBDebugCase : DeepStreamDebugger() {
    @Test
    fun `debug select from nb his`() {
        val context = initEnv()

        assertNotNull(context)

        //HMISW2003.KSMC
        //HMISW2003.ZGXX
        //HMISW2003.MZGHXX
        //HMISW2003.SYSVAR
        //HMISW2003.CFFL
        //HMISW2003.YZYFB
        //HMISW2003.GYFSB
        //HMISW2003.YPJX
        //HMISW2003.YZYFB
        //HMISW2003.MZCFMXXX
        //HMISW2003.YPMCQT
        //HMISW2003.MZCFXX
        //HMISW2003.YPMC
        //HMISW2003.MZSFXX
        //KTHIS.KTDOC_DIAGNOSE
        //KTHIS.KTDOC_CASE_HISTORY
        //KTHIS.KTDOC_WORK_LOG
        //KTHIS.KTDOC_PRESCRIPTION
        //KTHIS.KTDOC_PRESCRIPTION_LIST


        createTable(context, "DIP", TableContext("DIP", "KTDOC_PRESCRIPTION_LIST", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTDOC_PRESCRIPTION", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "YPMC", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTDOC_WORK_LOG", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "YPMCQT", TableContext.TABLE_TYPE_DIM))

        createTable(context, "DIP", TableContext("DIP", "KSMC", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "ZGXX", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "MZGHXX", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "SYSVAR", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "CFFL", TableContext.TABLE_TYPE_DIM))

        createTable(context, "DIP", TableContext("DIP", "YZYFB", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "GYFSB", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "YPJX", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "MZCFMXXX", TableContext.TABLE_TYPE_DIM))

        createTable(context, "DIP", TableContext("DIP", "MZCFXX", TableContext.TABLE_TYPE_KS))
        createTable(context, "DIP", TableContext("DIP", "MZSFXX", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTDOC_DIAGNOSE", TableContext.TABLE_TYPE_DIM))
        createTable(context, "DIP", TableContext("DIP", "KTDOC_CASE_HISTORY", TableContext.TABLE_TYPE_DIM))

//        executeSQL(context,SQL["queryCF"]!!).print()
        val table = executeQuery(context, SQL["queryCF"]!!)

        context.tableEnv.createTemporaryView("DTable", table)

        createTable(context, "eigtest", TableContext("", "S_CLI_RECIPE", TableContext.TABLE_TYPE_SINK))

        Files.asCharSink(File("/Users/starlesscity/workspace/product/opensource/deepstream/sqls_all.xml"), Charset.defaultCharset()).write(outputSqlDocument())

        val listTable = context.tableEnv.from("S_CLI_RECIPE")

        val sourceType = dataByTable(table.resolvedSchema)
        println("--${sourceType.fields.map { "\"$it\"" }.toList()}")
        println("--${sourceType.types.map { "\"$it\"" }.toList()}")

        executeSQL(
            context, SQL["insert"]!!
        ).print()

    }
}