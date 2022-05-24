package com.rui.ds

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.junit.Test

internal class SqlTest {

    @Test
    fun `flink sql test`() {
        val fsSettings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build()
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 1
        val tableEnv = StreamTableEnvironment.create(env, fsSettings)

        tableEnv.config.sqlDialect = SqlDialect.DEFAULT

        // 数据源表
        val sourceDDL = """
 CREATE TABLE r_mpi_patientinfo (
AUTO_ID VARCHAR,
LP_MPI_PATIENT VARCHAR,
PATIENT_ID VARCHAR,
NAME VARCHAR,
SEX_CODE VARCHAR,
SEX_NAME VARCHAR,
BIRTH_DATE TIMESTAMP(3),
PERSON_ID VARCHAR,
OCCUP_CODE VARCHAR,
OCCUP_NAME VARCHAR,
EMAIL VARCHAR,
NATIONALITY_CODE VARCHAR,
NATIONALITY_NAME VARCHAR,
NATION_CODE VARCHAR,
NATION_NAME VARCHAR,
EDUCATION_CODE VARCHAR,
EDUCATION_NAME VARCHAR,
MARRIAGE_CODE VARCHAR,
MARRIAGE_NAME VARCHAR,
BLOOD_TYPE_CODE VARCHAR,
BLOOD_TYPE_NAME VARCHAR,
BLOOD_RH_CODE VARCHAR,
BLOOD_RH_NAME VARCHAR,
PATIENT_BELONG_CODE VARCHAR,
PATIENT_BELONG_NAME VARCHAR,
PLACETYPE_CODE VARCHAR,
PLACETYPE_NAME VARCHAR,
REGISTERED_ADDRESS VARCHAR,
HOME_ADDRESS VARCHAR,
PROVINCE_NAME VARCHAR,
CITY_NAME VARCHAR,
COUNTY_NAME VARCHAR,
PHONE_TEL VARCHAR,
MOBILE_TEL VARCHAR,
POST_CODE VARCHAR,
WORK_UNIT VARCHAR,
WORK_PLACE VARCHAR,
WORK_TEL VARCHAR,
WORK_POSTCODE VARCHAR,
CONTACTS_NAME VARCHAR,
RELATION_CODE VARCHAR,
RELATION_NAME VARCHAR,
CONTACTS_TEL VARCHAR,
CONTACTS_WORK_PLACE VARCHAR,
CONTACTS_HOME_ADDRESS VARCHAR,
ID_TYPE_CODE VARCHAR,
ID_TYPE_NAME VARCHAR,
MEDICAL_TYPE_CODE VARCHAR,
MEDICAL_TYPE_NAME VARCHAR,
MEDICAL_ID VARCHAR,
VAAC_ID VARCHAR,
HEALTH_ID VARCHAR,
OFFICER_ID VARCHAR,
PASSPORT_ID VARCHAR,
OTHER_ID VARCHAR,
OPER_CODE VARCHAR,
OPER_NAME VARCHAR,
SOURCE_SYS VARCHAR,
CREATE_DTIME TIMESTAMP(3),
UPDATE_DTIME TIMESTAMP(3),
RECORD_DTIME TIMESTAMP(3),
RECORD_UPDATE_DTIME TIMESTAMP(3),
ORG_ID VARCHAR,
ORG_NAME VARCHAR,
NATIVE_CODE VARCHAR,
NATIVE_NAME VARCHAR,
NEWBORN_FLAG VARCHAR,
BIRTH_PLACE VARCHAR
) 
 WITH (
'connector' = 'jdbc',
'url' = 'jdbc:mysql://192.168.3.168:3216/eigdb?useSSL=false&autoReconnect=true',
'table-name' = 'r_mpi_patientinfo',
'username' = 'reseig',
'password' = 'mu-xPL7C',
'driver' = 'com.mysql.cj.jdbc.Driver',
'lookup.cache.max-rows' = '5000',
'lookup.cache.ttl' = '5min')"""


        val selectSql = """
            SELECT A.AUTO_ID AS AUTO_ID,
                   '' AS BATCH_NUM, --批次号 不填
                   '' AS UPLOAD_TIME, --系统上传时间 不填
                   A.PATIENT_ID AS LOCAL_ID, --人员在本地域唯一 必填
                   '' AS RESULTDESC, --处理结果描述 不填
                   '' AS RESULTCODE, --处理结果代码 不填
                   A.LP_CLI_FEE AS SERIALNUM_ID, --流水号 不填
                   '' AS UNIQUEID, --文档唯一ID 必填
                   '' AS PID, --主索引唯一ID 不填
                   '' AS BUSINESS_RELATION_ID, --业务关联ID 不填
                   '' AS BUSINESS_ACTIVE_TYPE, --业务活动类别 不填
                   '' AS BUSINESS_ACTIVE_DES, --业务活动类别描述 不填
                   A.OUTP_NO AS BUSINESS_ID, --业务流水号 必填
                   '' AS BASIC_ACTIVE_TYPE, --基本活动类别 不填
                   '' AS BASIC_ACTIVE_DES, --基本活动类别描述 不填
                   A.INVOICE_NO AS BASIC_ACTIVE_ID, --基本活动流水号 必填
                   '' AS ORGANIZATION_CODE, --组织机构代码 必填
                   '' AS ORGANIZATION_NAME, --组织机构名称 必填
                   '' AS DOMAIN_CODE, --域代码 必填
                   '' AS DOMAIN_NAME, --域名称 必填
                   '' AS VER, --版本号 不填
                   '' AS VERDES, --版本描述 不填
                   '' AS REGION_IDEN, --地区标识 必填
                   '' AS DATA_SECURITY, --数据密级 不填
                   '' AS RECORD_IDEN, --记录标识 必填
                   '' AS CREATE_DATE, --新建时间 不填
                   sysdate() AS UPDATE_DATE, --更新时间 不填
                   A.UPDATE_DTIME AS DATAGENERATE_DATE, --业务数据产生时间 必填
                   B.PATIENT_ID AS DE01_00_009_00, --健康档案标识符
                   B.NAME AS DE02_01_039_00, --姓名 必填
                   B.SEX_CODE AS DE02_01_040_00, --性别代码 必填
                   B.SEX_NAME AS AP02_01_102_01, --性别 必填
                   B.BIRTH_DATE AS DE02_01_005_01, --出生日期 必填
                   '' AS DE02_01_032_00, --实足年龄 必填
                   A.OUTP_NO AS DE01_00_010_00, --就诊流水号 必填
                   A.RECIPE_NO AS AP01_00_022_00, --处方号/申请单号 必填
                   A.TRANS_TYPE AS AP07_00_031_00, --交易类别 必填
                   A.INVOICE_NO AS AP07_00_016_00, --发票编号 必填
                   A.FEE_DTIME AS AP07_00_030_00, --计费时间
                   A.RECIPE_DOC_DEPTCODE AS AP08_10_055_01, --计费科室代码
                   A.RECIPE_DOC_DEPTNAME AS AP08_10_055_02, --计费科室名称
                   A.FEE_DTIME AS AP07_00_026_00, --收费时间/退费时间 必填
                   A.FEE_TYPE_CODE AS AP06_00_290_01, --费用项目类别代码 必填
                   A.ITEM_CODE AS AP07_00_017_00, --费用明细项目代码 必填
                   A.ITEM_NAME AS AP07_00_018_00, --费用明细项目名称 必填
                   A.UNIT AS AP07_00_019_00, --费用明细项目单位 必填
                   A.UNIT_PRICE AS AP07_00_020_00, --费用明细项目单价 必填
                   A.QTY AS AP07_00_021_00, --费用明细项目数量 必填
                   A.TOT_COST AS AP07_00_022_00, --费用明细项目金额 必填
                   A.AUTO_ID AS DE08_10_052_00, --就诊机构代码 必填
                  concat_ws("||",A.AUTO_ID,sysdate())   AS AP08_10_013_04, --就诊机构名称 必填
                   A.EXEC_DEPTCODE AS AP08_10_025_01, --收费科室代码 必填
                   A.EXEC_DEPTNAME AS DE08_10_026_00 ,--收费科室名称 必填
            A.UPDATE_DTIME  AS         RBUSINESS_DATE
              FROM R_CLI_FEE_DETAIL A
              LEFT JOIN R_MPI_PATIENTINFO B ON A.LK_MPI_PATIENT = B.LP_MPI_PATIENT
             
             WHERE A.UPDATE_DTIME >= str_to_date('${'$'}{BEGIN_DATE}','%Y-%m-%d %H:%i:%S')
               AND A.UPDATE_DTIME <str_to_date('${'$'}{END_DATE}','%Y-%m-%d %H:%i:%S')

        """.trimIndent()

        // 简单的聚合处理
        // 简单的聚合处理
        val transformSQL = "select * from r_mpi_patientinfo"

        tableEnv.executeSql(sourceDDL)

        val result = tableEnv.executeSql(transformSQL)
        result.print()

        env.execute("mySQL-to-Hudi")
    }

}