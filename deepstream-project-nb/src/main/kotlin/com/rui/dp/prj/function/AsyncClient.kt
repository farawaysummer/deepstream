package com.rui.dp.prj.function

import com.rui.dp.prj.job.DeepStreamHelper.getSql
import com.rui.dp.prj.job.DeepStreamHelper.loadDatasource
import com.rui.dp.prj.job.DeepStreamHelper.loadSql
import com.rui.ds.datasource.DatabaseSources.getConnection
import org.apache.commons.compress.utils.Lists
import org.apache.flink.types.Row
import org.apache.flink.types.RowKind
import java.math.BigDecimal
import java.sql.SQLException
import java.util.concurrent.CompletableFuture

class AsyncClient {
    init {
        loadDatasource()
        loadSql()
    }

    fun query(key: Row): CompletableFuture<Collection<Row?>> {
        return CompletableFuture.supplyAsync { queryDB(key) }
    }

    private fun queryDB(row: Row): List<Row?> {
        if (row.kind == RowKind.UPDATE_BEFORE) {
            return emptyList()
        }

        try {
            getConnection("HIS").use { connection ->
                val sql = getSql("selectHIS")
                val rows: MutableList<Row?> = Lists.newArrayList()
                val statement = connection!!.prepareStatement(sql)
                //AND A.NO= ? AND A.KFDM = ? AND A.LX = ? AND A.SFNO = ?
                val NO = row.getField("NO")
                val KFDM = row.getField("KFDM")
                val LX = row.getField("LX")
                val SFNO = row.getField("SFNO")
                statement.setObject(1, NO)
                statement.setObject(2, KFDM)
                statement.setObject(3, LX)
                statement.setObject(4, SFNO)

                println("====Start execute query")
                val result = statement.executeQuery()

                while (result.next()) {
                    val values = fields.associateBy({ it }, { result.getObject(it) }).mapValues { (_, value) ->
                        if (value is BigDecimal) {
                            value.toString()
                        } else {
                            value
                        }
                    }
                    val newRow = Row.withNames()
//                    newRow.kind = row.kind
                    fields.forEach { newRow.setField(it, values[it]) }

                    rows.add(newRow)
                }
                println("====Finish execute query with result:$rows")
                return rows
            }
        } catch (e: SQLException) {
            e.printStackTrace()
            throw RuntimeException(e)
        }
    }

    companion object {
        val fields = arrayOf(
            "ORG_CODE",
            "AUTO_ID",
            "LP_CLI_RECIPE",
            "LK_CLI_RECIPE",
            "LK_CLI_SEE",
            "LK_MPI_PATIENT",
            "PATIENT_ID",
            "OUTP_ID",
            "OUTP_NO",
            "RECIPE_NO",
            "RECIPE_DTIME",
            "RECIPE_DEPT_CODE",
            "RECIPE_DEPT_NAME",
            "RECIPE_DOC_CODE",
            "RECIPE_DOC_NAME",
            "DRUG_FLAG",
            "ITEM_CODE",
            "ITEM_NAME",
            "ITEM_TYPE_CODE",
            "ITEM_TYPE_NAME",
            "UNIT_PRICE",
            "QTY",
            "UNIT",
            "SPEC",
            "DRUG_RECP_TYPECODE",
            "DRUG_RECP_TYPENAME",
            "DRUG_USE_FREQCODE",
            "DRUG_USE_FREQNAME",
            "DRUG_EACH_DOSE",
            "DRUG_DOSE_UNIT",
            "DRUG_USAGE_CODE",
            "DRUG_USAGE_NAME",
            "DOSAGE_FORM_CODE",
            "DOSAGE_FORM_NAME",
            "DRUG_TOTAL_DOSE",
            "DRUG_TOTAL_UNIT",
            "DRUG_START_DTIME",
            "DRUG_END_DTIME",
            "DURG_SEND_DTIME",
            "DRUG_USE_DAYS",
            "DRUG_PACKAGE_QTY",
            "DRUG_PACKAGE_UNIT",
            "DRUG_MIN_UNIT",
            "DRUG_BASE_DOSE",
            "DRUG_MAIN_FLAG",
            "GROUP_NO",
            "DRUG_HERB_FOOTNOTES",
            "HERB_DOSE_QTY",
            "CHARGE_FLAG",
            "CHARGE_DTIME",
            "DRUG_SKIN_FLAG",
            "DRUG_SKIN_RESULT",
            "INSPECTION_PART_CODE",
            "INSPECTION_PART_NAME",
            "EMER_FLAG",
            "APPLY_NO",
            "REPORT_NO",
            "NOTE",
            "USE_TIMES",
            "PRODUCTION_PLACE",
            "PRODUCTION_DTIME",
            "VALID_DTIME",
            "MEDIC_CODE",
            "MEDIC_CODE_SAFETY",
            "MEDIC_SELF",
            "MEDIC_SAFETY",
            "CREATE_DTIME",
            "UPDATE_DTIME",
            "RECORD_DTIME",
            "RECORD_UPDATE_DTIME",
            "INFOR_SOURCE",
            "INFOR_IDENTIFICATION_CODE",
            "INFOR_VALID_DATE",
            "INFOR_REVIEWER_NAME",
            "INFOR_REVIEWER_ID",
            "INFOR_REVIEW_TIME",
            "INFOR_PRESCRIBE_TIME",
            "INFOR_DISPENSE_TIME",
            "INFOR_STATUS",
            "INFOR_CANCEL_TIME",
            "INFOR_PRES_FEE",
            "INFOR_PRESCRIBE_EMP_NAME",
            "INFOR_PRESCRIBE_EMP_ID",
            "INFOR_DISPENSE_EMP_NAME",
            "INFOR_DISPENSE_EMP_ID",
            "INFOR_REMARK",
            "INFOR_CIRCULATE_FLAG",
            "LIST_CHARGE_ID",
            "LIST_IS_MEDI",
            "LIST_IS_MAIN",
            "LIST_ANTIBIOS_FLAG",
            "LIST_SPIRIT_FLAG",
            "LIST_ANAESTHETIZTION",
            "LIST_BASE_FLAG",
            "LIST_VACCINE_FLAG",
            "LIST_INFUSION_FLAG",
            "LIST_WITHDRAWAL_FLAG",
            "LIST_DOSAGE_FORM",
            "LIST_SPECIFICATION_UNIT",
            "LIST_SPECIFICATION_",
            "LIST_DISPENSE_NUMBER",
            "LIST_DISPENSE_UNIT",
            "LIST_DOSE_UNITNAME",
            "LIST_DOSE_UNITID",
            "LIST_USEUNIT_ID",
            "LIST_IS_SKINTESTDRUG",
            "LIST_REVISE_FLAG",
            "LIST_POISON_FLAG",
            "LIST_POISONMADE_FLAG",
            "LIST_MEDI_TIMING",
            "LIST_DRUGUSE_TOTAL",
            "LIST_DRUG_SOURCE",
            "LIST_MEDI_SPEED",
            "LIST_LIMITED_TIME",
            "LIST_MEDI_PURPOSE",
            "LIST_PROJECT_AMOUNT",
            "LIST_ANTI_TUMOR_DRUG"
        )
    }
}