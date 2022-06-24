package com.rui.dp.prj

import com.google.common.base.Strings
import com.rui.dp.prj.job.DeepStreamHelper
import com.rui.dp.prj.job.DeepStreamHelper.executeQuery
import com.rui.dp.prj.job.DeepStreamHelper.executeSQL
import com.rui.dp.prj.job.DeepStreamHelper.getSql
import com.rui.ds.ProcessContext
import com.rui.ds.StreamDataTypes
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.table.runtime.typeutils.TimestampDataTypeInfo
import java.util.concurrent.TimeUnit

class NBStreamProcess {
    private var context: ProcessContext = DeepStreamHelper.initEnv()

    init {
    }

    fun createTables() {
        // create table
        for (tableName in tables) {
            val tableSql = getSql(tableName)
            if (!Strings.isNullOrEmpty(tableSql)) {
                DeepStreamHelper.executeSQL(context, tableSql!!)
            }
        }
    }

    fun startProcess() {
        val mainSql = getSql("querySingle")
        val mainResult = executeQuery(context, mainSql!!)

        val rowDataStream = context.tableEnv.toChangelogStream(mainResult)

        val asyncFunction = SampleAsyncFunction()

        val queryResult = AsyncDataStream.unorderedWait(
            rowDataStream,
            asyncFunction,
            30000, TimeUnit.SECONDS
        )
            .returns(
                streamDataType.toTypeInformation()
            )

        context.tableEnv.createTemporaryView("DTable", queryResult)
//
//        DeepStreamHelper.executeSQL(context, "select * from DTable").print()
        val insertSql = getSql("insertCF")
//
//
//        context.tableEnv.createTemporaryView("DTable", outputTable)
//
        executeSQL(context, insertSql!!)

    }

    companion object {
        private val tables = listOf(
            "KSMC",
            "ZGXX",
            "MZGHXX",
            "SYSVAR",
            "CFFL",

            "YZYFB",
            "GYFSB",
            "YPJX",
            "MZCFMXXX",
            "YPMCQT",

            "MZCFXX",
            "YPMC",
            "MZSFXX",
            "KTDOC_DIAGNOSE",

            "KTDOC_CASE_HISTORY",
            "KTDOC_WORK_LOG",
            "KTDOC_PRESCRIPTION",
            "KTDOC_PRESCRIPTION_LIST",

            "S_CLI_RECIPE"
        )

        private val typeMap = mapOf(
            "ORG_CODE" to "VARCHAR2",
            "AUTO_ID" to "VARCHAR2",
            "LP_CLI_RECIPE" to "VARCHAR2",
            "LK_CLI_RECIPE" to "VARCHAR2",
            "LK_CLI_SEE" to "VARCHAR2",
            "LK_MPI_PATIENT" to "VARCHAR2",
            "PATIENT_ID" to "VARCHAR2",
            "OUTP_ID" to "VARCHAR2",
            "OUTP_NO" to "VARCHAR2",
            "RECIPE_NO" to "VARCHAR2",
            "RECIPE_DTIME" to "DATE",
            "RECIPE_DEPT_CODE" to "VARCHAR2",
            "RECIPE_DEPT_NAME" to "VARCHAR2",
            "RECIPE_DOC_CODE" to "VARCHAR2",
            "RECIPE_DOC_NAME" to "VARCHAR2",
            "DRUG_FLAG" to "CHAR",
            "ITEM_CODE" to "VARCHAR2",
            "ITEM_NAME" to "VARCHAR2",
            "ITEM_TYPE_CODE" to "VARCHAR2",
            "ITEM_TYPE_NAME" to "VARCHAR2",
            "UNIT_PRICE" to "VARCHAR2",
            "QTY" to "VARCHAR2",
            "UNIT" to "VARCHAR2",
            "SPEC" to "VARCHAR2",
            "DRUG_RECP_TYPECODE" to "VARCHAR2",
            "DRUG_RECP_TYPENAME" to "VARCHAR2",
            "DRUG_USE_FREQCODE" to "VARCHAR2",
            "DRUG_USE_FREQNAME" to "VARCHAR2",
            "DRUG_EACH_DOSE" to "VARCHAR2",
            "DRUG_DOSE_UNIT" to "VARCHAR2",
            "DRUG_USAGE_CODE" to "VARCHAR2",
            "DRUG_USAGE_NAME" to "VARCHAR2",
            "DOSAGE_FORM_CODE" to "VARCHAR2",
            "DOSAGE_FORM_NAME" to "VARCHAR2",
            "DRUG_TOTAL_DOSE" to "VARCHAR2",
            "DRUG_TOTAL_UNIT" to "VARCHAR2",
            "DRUG_START_DTIME" to "CHAR",
            "DRUG_END_DTIME" to "CHAR",
            "DURG_SEND_DTIME" to "DATE",
            "DRUG_USE_DAYS" to "VARCHAR2",
            "DRUG_PACKAGE_QTY" to "VARCHAR2",
            "DRUG_PACKAGE_UNIT" to "VARCHAR2",
            "DRUG_MIN_UNIT" to "CHAR",
            "DRUG_BASE_DOSE" to "CHAR",
            "DRUG_MAIN_FLAG" to "VARCHAR2",
            "GROUP_NO" to "VARCHAR2",
            "DRUG_HERB_FOOTNOTES" to "CHAR",
            "HERB_DOSE_QTY" to "VARCHAR2",
            "CHARGE_FLAG" to "CHAR",
            "CHARGE_DTIME" to "DATE",
            "DRUG_SKIN_FLAG" to "VARCHAR2",
            "DRUG_SKIN_RESULT" to "VARCHAR2",
            "INSPECTION_PART_CODE" to "CHAR",
            "INSPECTION_PART_NAME" to "CHAR",
            "EMER_FLAG" to "CHAR",
            "APPLY_NO" to "CHAR",
            "REPORT_NO" to "CHAR",
            "NOTE" to "VARCHAR2",
            "USE_TIMES" to "CHAR",
            "PRODUCTION_PLACE" to "VARCHAR2",
            "PRODUCTION_DTIME" to "CHAR",
            "VALID_DTIME" to "CHAR",
            "MEDIC_CODE" to "VARCHAR2",
            "MEDIC_CODE_SAFETY" to "VARCHAR2",
            "MEDIC_SELF" to "CHAR",
            "MEDIC_SAFETY" to "CHAR",
            "CREATE_DTIME" to "DATE",
            "UPDATE_DTIME" to "DATE",
            "RECORD_DTIME" to "CHAR",
            "RECORD_UPDATE_DTIME" to "CHAR",
            "INFOR_SOURCE" to "CHAR",
            "INFOR_IDENTIFICATION_CODE" to "VARCHAR2",
            "INFOR_VALID_DATE" to "VARCHAR2",
            "INFOR_REVIEWER_NAME" to "VARCHAR2",
            "INFOR_REVIEWER_ID" to "VARCHAR2",
            "INFOR_REVIEW_TIME" to "DATE",
            "INFOR_PRESCRIBE_TIME" to "DATE",
            "INFOR_DISPENSE_TIME" to "DATE",
            "INFOR_STATUS" to "CHAR",
            "INFOR_CANCEL_TIME" to "VARCHAR2",
            "INFOR_PRES_FEE" to "VARCHAR2",
            "INFOR_PRESCRIBE_EMP_NAME" to "VARCHAR2",
            "INFOR_PRESCRIBE_EMP_ID" to "VARCHAR2",
            "INFOR_DISPENSE_EMP_NAME" to "VARCHAR2",
            "INFOR_DISPENSE_EMP_ID" to "VARCHAR2",
            "INFOR_REMARK" to "VARCHAR2",
            "INFOR_CIRCULATE_FLAG" to "VARCHAR2",
            "LIST_CHARGE_ID" to "VARCHAR2",
            "LIST_IS_MEDI" to "CHAR",
            "LIST_IS_MAIN" to "VARCHAR2",
            "LIST_ANTIBIOS_FLAG" to "CHAR",
            "LIST_SPIRIT_FLAG" to "CHAR",
            "LIST_ANAESTHETIZTION" to "VARCHAR2",
            "LIST_BASE_FLAG" to "VARCHAR2",
            "LIST_VACCINE_FLAG" to "CHAR",
            "LIST_INFUSION_FLAG" to "VARCHAR2",
            "LIST_WITHDRAWAL_FLAG" to "CHAR",
            "LIST_DOSAGE_FORM" to "VARCHAR2",
            "LIST_SPECIFICATION_UNIT" to "VARCHAR2",
            "LIST_SPECIFICATION_" to "VARCHAR2",
            "LIST_DISPENSE_NUMBER" to "VARCHAR2",
            "LIST_DISPENSE_UNIT" to "VARCHAR2",
            "LIST_DOSE_UNITNAME" to "VARCHAR2",
            "LIST_DOSE_UNITID" to "VARCHAR2",
            "LIST_USEUNIT_ID" to "VARCHAR2",
            "LIST_IS_SKINTESTDRUG" to "VARCHAR2",
            "LIST_REVISE_FLAG" to "CHAR",
            "LIST_POISON_FLAG" to "VARCHAR2",
            "LIST_POISONMADE_FLAG" to "VARCHAR2",
            "LIST_MEDI_TIMING" to "VARCHAR2",
            "LIST_DRUGUSE_TOTAL" to "VARCHAR2",
            "LIST_DRUG_SOURCE" to "VARCHAR2",
            "LIST_MEDI_SPEED" to "VARCHAR2",
            "LIST_LIMITED_TIME" to "VARCHAR2",
            "LIST_MEDI_PURPOSE" to "VARCHAR2",
            "LIST_PROJECT_AMOUNT" to "VARCHAR2",
            "LIST_ANTI_TUMOR_DRUG" to "CHAR",
        )

        val streamDataType: StreamDataTypes

        init {
            val allTypes = typeMap.mapValues {(_, value) ->
                when (value) {
                    "String" -> BasicTypeInfo.STRING_TYPE_INFO
                    "Integer" -> BasicTypeInfo.INT_TYPE_INFO
                    "LocalDate" -> BasicTypeInfo.DATE_TYPE_INFO
                    "Double" -> BasicTypeInfo.DOUBLE_TYPE_INFO
                    "VARCHAR2" -> BasicTypeInfo.STRING_TYPE_INFO
                    "CHAR" -> BasicTypeInfo.STRING_TYPE_INFO
                    "DATE" -> BasicTypeInfo.DATE_TYPE_INFO

                    "NUMBER" -> BasicTypeInfo.BIG_DEC_TYPE_INFO
                    "Timestamp" -> TimestampDataTypeInfo(6)
                    else -> BasicTypeInfo.STRING_TYPE_INFO
                } as TypeInformation<*>
            }

            streamDataType = StreamDataTypes(allTypes.keys.toTypedArray(), allTypes.values.toTypedArray())
        }
    }
}