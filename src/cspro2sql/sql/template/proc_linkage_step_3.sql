DELIMITER ;;
CREATE PROCEDURE @SCHEMA.`proc_linkage_step_3`()
BEGIN

    INSERT INTO @SCHEMA.linkage_log (id_object) VALUES ('proc_linkage_step_3');

    DROP TABLE IF EXISTS @SCHEMA.linkage_step_3;

    CREATE TABLE @SCHEMA.linkage_step_3 AS
        SELECT
            pes.ID_INDIV AS PES_ID,
            pes.ID_QUEST AS PES_ID_QUEST,
            pes.NAME1 AS PES_NAME1,
            pes.NAME2 AS PES_NAME2,
            pes.NAME3 AS PES_NAME3,
            pes.AGE AS PES_AGE,
            pes.SEX AS PES_SEX,
            pes.RELIGION AS PES_RELIGION,
            pes.RELAT AS PES_RELAT,
            pes.GRADE AS PES_GRADE,
            pes.TONGUE AS PES_TONGUE,
            pes.MARITAL AS PES_MARITAL,
            cens.ID_INDIV AS CENS_ID,
            cens.ID_QUEST AS CENS_ID_QUEST,
            cens.NAME1 AS CENS_NAME1,
            cens.NAME2 AS CENS_NAME2,
            cens.NAME3 AS CENS_NAME3,
            cens.AGE AS CENS_AGE,
            cens.SEX AS CENS_SEX,
            cens.RELIGION AS CENS_RELIGION,
            cens.RELAT AS CENS_RELAT,
            cens.GRADE AS CENS_GRADE,
            cens.TONGUE AS CENS_TONGUE,
            cens.MARITAL AS CENS_MARITAL,
            pes.COD_EA AS COD_EA,
            "3" as STEP
        FROM
            @SCHEMA.linkage_pes_matching_var pes JOIN @SCHEMA.linkage_cens_matching_var cens ON
                pes.FIXED_NAME_1 = cens.FIXED_NAME_1 AND
                pes.FIXED_NAME_2 = cens.FIXED_NAME_2 AND
                pes.FIXED_NAME_3 = cens.FIXED_NAME_3 AND
                pes.SEX = cens.SEX AND
                pes.RELIGION = cens.RELIGION AND
                pes.RELAT = cens.RELAT AND
                pes.GRADE = cens.GRADE AND
                pes.TONGUE = cens.TONGUE AND
                pes.MARITAL = cens.MARITAL AND
                pes.COD_EA = cens.COD_EA
        WHERE
            NOT pes.ID_INDIV IN (SELECT ID_INDIVIDUAL_PES FROM @SCHEMA.linked_record);

    ALTER TABLE @SCHEMA.linkage_step_3 ADD PRIMARY KEY (PES_ID, CENS_ID);

END ;;
DELIMITER ;

