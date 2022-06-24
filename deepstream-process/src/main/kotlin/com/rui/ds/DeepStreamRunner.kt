package com.rui.ds

object DeepStreamRunner {
//    @JvmStatic
//    function main(args: Array<String>) {
//
//        // init env
//        val sourceProperties = Properties()
//        sourceProperties.load(javaClass.getResource("/source.properties")?.openStream())
//        val sourceConfig = DataSourceConfig.createFromProperties("source", sourceProperties)
//        DataSource.registryDataSource(sourceConfig)
//
//        val targetProperties = Properties()
//        targetProperties.load(javaClass.getResource("/target.properties")?.openStream())
//        val targetConfig = DataSourceConfig.createFromProperties("target", targetProperties)
//        DataSource.registryDataSource(targetConfig)
//
//        val factTable = TableContext(
//            catalog = "eigdb",
//            tableName = "r_mpi_patientinfo",
//            tableType = "jdbc"
//        )
//
//        val dimTable = TableContext(
//            catalog = "eigdb",
//            tableName = "r_dict_medicalpayment",
//            tableType = "dim"
//        )
//
//        val targetTable = TableContext(
//            catalog = "test",
//            tableName = "f_result",
//            tableType = "jdbc"
//        )
//
//
//        // 创建job
//        val job = DeepStreamJob()
//
//        // 创建表输入步骤
//        val tableInput = TableInputStep(
//            StepMapConfig(
//                mapOf(
//                    "Tables" to arrayOf(factTable, dimTable),
//                    "DataSourceConfig" to sourceConfig,
//                    "InputSql" to
//                            """
//                                select r.PATIENT_ID,
//                                       r.SEX_NAME,
//                                        r.MEDICAL_TYPE_CODE,
//                                        r.NAME,
//                                        (select d.NAME from ${dimTable.tableName} as d where r.MEDICAL_TYPE_CODE = d.CODE) as nc
//                                from ${factTable.tableName} as r limit 10
//        """.trimIndent()
//                )
//            )
//        )
//        job.addStep(tableInput)
//
//        val fieldSelect = FieldSelectStep(
//            StepMapConfig(
//                mapOf(
//                    "Fields" to arrayOf("PATIENT_ID", "MEDICAL_TYPE_CODE", "NAME", "nc")
//                )
//            )
//        )
//        job.addStep(fieldSelect)
//
//        val printTable = PrintStep()
//        job.addStep(printTable)
//
//        val tableOutput = TableOutputStep(
//            StepMapConfig(
//                mapOf(
//                    "OutputTable" to targetTable,
//                    "DataSourceConfig" to targetConfig,
//                    "OutputFields" to arrayOf("PATIENT_ID", "MEDICAL_TYPE_CODE", "NAME", "nc")
//                )
//            )
//        )
//        job.addStep(tableOutput)
//
//        job.start()
//    }
}