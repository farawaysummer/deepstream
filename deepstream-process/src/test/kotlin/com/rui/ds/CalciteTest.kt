package com.rui.ds

import org.apache.calcite.sql.parser.SqlParser
import org.junit.Test


internal class CalciteTest {

    @Test
    fun `calcite test schema`() {
        val parser = SqlParser.create("""
            SELECT '01' AS ORG_CODE, --医疗机构代码
	   A.INPATIENT_NO AS BASE_ID, --源表物理主键
	   A.CARD_NO AS PATIENT_ID, --患者编号
	   A.PATIENT_NO AS INP_ID, --住院号
	   A.INPATIENT_NO AS INP_NO, --住院流水号
	   A.IN_TIMES AS INHOS_TIMES, --住院次数
	   A.NAME AS NAME, --姓名
	   A.SEX_CODE AS SEX_CODE, --性别代码
	   DECODE(A.SEX_CODE, 'F', '女', 'M', '男', '') AS SEX_NAME, --性别名称
	   A.BIRTHDAY AS BIRTH_DATE, --出生日期
	   A.IDENNO AS PERSON_ID, --身份证号
	   A.MARI AS MARRIAGE_CODE, --婚姻状况代码
	   DECODE(A.MARI, 'M', '已婚', 'S', '未婚') AS MARRIAGE_NAME, --婚姻状况
	   A.PROF_CODE OCCUP_CODE, --职业代码
       (SELECT AA.BLABLA
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'PROFESSION'
		   AND AA.CODE = A.PROF_CODE) , 
	   (SELECT AA.NAME
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'PROFESSION'
		   AND AA.CODE = A.PROF_CODE) OCCUP_NAME, --职业名称
	   A.IN_DATE AS INHOS_DTIME, --入院时间
	   A.DEPT_CODE AS INHOS_DEPT_CODE, --入院科室代码
	   A.DEPT_NAME AS INHOS_DEPT_NAME, --入院科室
	   A.NURSE_CELL_CODE AS INHOS_NRS_CODE, --入院病区代码
	   A.NURSE_CELL_NAME AS INHOS_NRS_NAME, --入院病区
	   A.BED_NO AS INHOS_BED_NO, --入院床号
	   A.CLINIC_DIAGNOSE AS CLINIC_DIAG_CODE, --门诊诊断代码
	   '' AS CLINIC_DIAG_NAME, --门诊诊断
	   '' AS INHOS_DIAG_CODE, --入院诊断代码
	   '' AS INHOS_DIAG_NAME, --入院诊断
	   A.IN_CIRCS AS INHOS_CIRCS_CODE, --入院情况代码
	   (SELECT AA.NAME
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'INCIRCS'
		   AND AA.CODE = A.IN_CIRCS) AS INHOS_CIRCS_NAME, --入院情况
	   A.IN_AVENUE AS INHOS_WAY_CODE, --入院途径代码
	   DECODE(A.IN_AVENUE, '1', '急诊', '2', '门诊', '3', '转科', '4', '转院') AS INHOS_WAY_NAME, --入院途径
	   '' AS INHOS_RESON_CODE, --入院原因代码
	   '' AS INHOS_RESON_NAME, --入院原因
	   A.IN_STATE INHOS_STATE_CODE, --在院状态代码
	   DECODE(A.IN_STATE,
			  'R',
			  '入院登记',
			  'I',
			  '病房接诊',
			  'B',
			  '出院登记',
			  'P',
			  '预约出院',
			  'N',
			  '无法退院',
			  'O',
			  '出院结算',
			  '') INHOS_STATE_NAME, --在院状态名称
	   REPLACE(A.HOME,CHR(0),'') HOME_ADDRESS, --家庭地址
	   A.HOME_TEL HOME_TEL, --家庭电话
	   A.HOME_ZIP HOME_POST, --家庭邮编
	   A.WORK_NAME WORK_ADDRESS, --工作地址
	   A.WORK_TEL WORK_TEL, --工作电话
	   A.WORK_ZIP WORK_POST, --工作邮编
	   A.LINKMAN_NAME LINK_NAME, --联系人姓名
	   A.LINKMAN_TEL LINK_TEL, --联系电话
	   A.LINKMAN_ADD LINK_ADDRESS, --联系人地址
	   A.RELA_CODE LINK_RELATION_CODE, --联系人关系代码
	   (SELECT AA.NAME
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'RELATIVE'
		   AND AA.CODE = A.RELA_CODE) LINK_RELATION_NAME, --联系人关系名称
	   A.BIRTH_AREA BIRTH_PLACE_CODE, --出生地代码
	   (SELECT AA.NAME
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'DIST'
		   AND AA.CODE = A.BIRTH_AREA) BIRTH_PLACE_NAME, --出生地名称
	   A.NATION_CODE NATION_CODE, --民族代码
	   (SELECT AA.NAME
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'NATION'
		   AND AA.CODE = A.NATION_CODE) NATION_NAME, --民族名称
	   '' NURSING_LEVEL_CODE, --护理级别代码
	   A.TEND NURSING_LEVEL_NAME, --护理级别名称
	   A.HOUSE_DOC_CODE HOUSE_DOC_CODE, --住院医师代码
	   A.HOUSE_DOC_NAME HOUSE_DOC_NAME, --住院医师名称
	   A.CHARGE_DOC_CODE ATTEND_DOC_CODE, --主治医师代码
	   A.CHARGE_DOC_NAME ATTEND_DOC_NAME, --主治医师名称
	   A.CHIEF_DOC_CODE CHICF_DOC_CODE, --主任医师代码
	   A.CHIEF_DOC_NAME CHICF_DOC_NAME, --主任医师名称
	   A.DUTY_NURSE_CODE DUTY_NURSE_CODE, --责任护士代码
	   A.DUTY_NURSE_NAME DUTY_NURSE_NAME, --责任护士名称
	   '' AS REMAIN_HOS_FLAG, --留观标识
	   '' AS INFECT_FLAG, --传染病标识
	   CASE
		   WHEN (TRUNC(A.IN_DATE) -
				TRUNC(NVL(A.BIRTHDAY, TO_DATE('2000-01-01', 'YYYY-MM-DD')))) <= 30 THEN
			'1'
		   ELSE
			'2'
	   END AS BABY_FLAG, --新生儿标识
	   A.OPER_CODE AS OPER_CODE, --操作人员代码
	   (SELECT AA.EMPL_NAME FROM LCHIS.COM_EMPLOYEE AA WHERE AA.EMPL_CODE = A.OPER_CODE) AS OPER_NAME, --操作人员
	   A.IN_DATE AS CREATE_DTIME, --业务数据产生时间
	   A.OPER_DATE AS UPDATE_DTIME, --业务数据更新时间
	   SYSDATE AS RECORD_CREATE_DTIME, --数据采集时间
	   SYSDATE AS RECORD_UPDATE_DTIME, --数据更新时间
	   A.PACT_CODE AS MEDICAL_TYPE_CODE,
	   A.PACT_NAME AS MEDICAL_TYPE_NAME,
	   A.BALANCE_COST + A.OWN_COST AS TOT_COST, --总费用
	   B.ZUCODE AS TREATMENT_GROUP_CODE,
	   B.ZUNAME AS TREATMENT_GROUP_NAME,
	   DECODE(A.CRITICAL_FLAG, '1', '1', '2', '1', '2') AS CRIT_FLAG, --危重标识
	   A.PREPAY_COST AS ADVANCE_COST, --预交金金额
	   A.OWN_COST AS UNSETTLED_COST, --未结算金额
	   B.TUISHOUXUDATE AS CANCEL_DTIME, -- 作废时间
	   CASE
		   WHEN (LENGTH(A.IDENNO) = 18 OR LENGTH(A.IDENNO) = 15) THEN
			'1'
		   ELSE
			'2'
	   END AS REAL_NAME_FLAG, -- 实名制标识 1是 2 否
	   A.MEMO AS REGION_CODE, -- 统筹区编码
	   (SELECT AA.NAME
		  FROM LCHIS.COM_DICTIONARY AA
		 WHERE AA.TYPE = 'REMARK'
		   AND AA.CODE = A.MEMO) AS REGION_NAME, --统筹区名称
	   NVL(A.EXT_CODE, '2') AS CANCEL_FLAG -- 作废标识
  FROM LCHIS.FIN_IPR_INMAININFO A
  LEFT JOIN LCHIS.FIN_IPR_INMAININFO_EXT B
	ON A.INPATIENT_NO = B.INPATIENT_NO
 WHERE ((A.IN_DATE >= TO_DATE('z', 'YYYY-MM-DD HH24:MI:SS') AND
	   A.IN_DATE < TO_DATE('b', 'YYYY-MM-DD HH24:MI:SS')) OR
	   (A.OPER_DATE >= TO_DATE('1', 'YYYY-MM-DD HH24:MI:SS') AND
	   A.OPER_DATE < TO_DATE('2', 'YYYY-MM-DD HH24:MI:SS')) OR
	   (A.OUT_DATE >= TO_DATE('1', 'YYYY-MM-DD HH24:MI:SS') - 20 AND
	   A.OUT_DATE < TO_DATE('2', 'YYYY-MM-DD HH24:MI:SS')) OR
	   (A.BALANCE_DATE >= TO_DATE('1', 'YYYY-MM-DD HH24:MI:SS') AND
	   A.BALANCE_DATE < TO_DATE('2', 'YYYY-MM-DD HH24:MI:SS')) OR
	   A.IN_STATE IN ('R', 'I')) -- 入院登记，病房接诊
   AND A.PATIENT_NO IS NOT NULL
        """.trimIndent())
        val node = parser.parseQuery()

        val columns = mutableSetOf<String>()
//        SQLUtils.extractSelectColumnsInSql(node, columns)
//        println(columns)
//        if (node is SqlSelect) {
//            val selectList = node.selectList
//            for (field in selectList) if (node is SqlSelect){
//                if (field.kind == SqlKind.AS) {
//                    val asName = (field as SqlCall).operand<SqlNode>(1)
//                    println(asName)
//                } else if (field.kind == SqlKind.SELECT) {
//                    println("===========")
//                }
//            }
//        }


//        val tables = mutableSetOf<String>()
//        SQLUtils.extractTablesInSql(node, tables)
//        println(tables)
//        println(node)
    }
}
