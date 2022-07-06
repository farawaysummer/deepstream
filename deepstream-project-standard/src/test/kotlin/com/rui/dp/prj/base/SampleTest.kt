package com.rui.dp.prj.base

import com.fasterxml.jackson.core.util.Separators
import com.google.common.base.Splitter

object SampleTest {
    @JvmStatic
    fun main(args: Array<String>) {

        val content = """
            `BASE_ID` VARCHAR,
`HOSP_ID` VARCHAR,
`HOSP_NAME` VARCHAR,
`BATCH_NO` VARCHAR,
`AUTO_ID` VARCHAR,
`PATIENT_ID` VARCHAR,
`OUTP_ID` VARCHAR,
`OUTP_NO` VARCHAR,
`NAME` VARCHAR,
`CLI_DEPT_CODE` VARCHAR,
`CLI_DEPT_NAME` VARCHAR,
`CLI_DTIME` DATE,
`CLI_DOCT_CODE` VARCHAR,
`CLI_DOCT_NAME` VARCHAR,
`DIAG_CODE` VARCHAR,
`DIAG_NAME` VARCHAR,
`CHIEF_COMPLIANT` VARCHAR,
`SYMPTOM_CODE` VARCHAR,
`SYMPTOM_NAME` VARCHAR,
`MEDICAL_HISTORY` VARCHAR,
`ALLERGIC_HISTORY` VARCHAR,
`CHECK_UP` VARCHAR,
`TREATMENT_PLAN` VARCHAR,
`SBP` VARCHAR,
`DBP` VARCHAR,
`HEIGHT` VARCHAR,
`WEIGHT` VARCHAR,
`CONSULT_QUESTION` VARCHAR,
`HEALTH_REQUEST` VARCHAR,
`HEALTH_EVALUATION` VARCHAR,
`OTHER_MEDICAL_TREATMENT` VARCHAR,
`DIAG_CODING_TYPE` VARCHAR,
`DIAG_NOTE` VARCHAR,
`REFERRAL_FLAG` VARCHAR,
`REFERRAL_NOTE` VARCHAR,
`TYPE` VARCHAR,
`RESULT` VARCHAR,
`CREATE_DTIME` DATE,
`UPDATE_DTIME` DATE,
`RECORD_DTIME` DATE,
`RECORD_UPDATE_DTIME` DATE,
`HOSP_ID_BAK` VARCHAR,
`HOSP_NAME_BAK` VARCHAR,
`OLD_CLI_DEPT_CODE` VARCHAR,
`OLD_CLI_DEPT_NAME` VARCHAR,
`OLD_MEDICAL_TPYE_CODE` VARCHAR,
`OLD_MEDICAL_TPYE_NAME` VARCHAR,
`TEMP` VARCHAR,
`CLINIC_END_TIME` DATE,
        """.trimIndent()


        val lines = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(content)
        println("<table>")
        for (line in lines) {

            val parts = line.split(" ")
            val fieldName = parts[0].substring(1, parts[0].length - 1)
            val fileType = parts[1]
            println("<field name=\"$fieldName\" type=\"$fileType\"/>")
        }
        println("</table>")
    }
}