CREATE OR REPLACE VIEW @SCHEMA.linked_individual AS
    SELECT
        lr.ID_INDIVIDUAL_PES as PES_ID,
        lr.ID_QUEST_PES as PES_ID_QUEST,
        pes.NAME1 as PES_NAME1,
        pes.NAME2 as PES_NAME2,
        pes.NAME3 as PES_NAME3,
        pes.GRADE as PES_GRADE,
        pes.MARITAL as PES_MARITAL,
        pes.RELAT as PES_RELAT,
        pes.RELIGION as PES_RELIGION,
        pes.SEX as PES_SEX,
        pes.TONGUE as PES_TONGUE,
        pes.AGE as PES_AGE,
        lr.ID_INDIVIDUAL_CENS as CENS_ID,
        lr.ID_QUEST_CENS as CENS_ID_QUEST,
        cens.NAME1 as CENS_NAME1,
        cens.NAME2 as CENS_NAME2,
        cens.NAME3 as CENS_NAME3,
        cens.GRADE as CENS_GRADE,
        cens.MARITAL as CENS_MARITAL,
        cens.RELAT as CENS_RELAT,
        cens.RELIGION as CENS_RELIGION,
        cens.SEX as CENS_SEX,
        cens.TONGUE as CENS_TONGUE,
        cens.AGE as CENS_AGE,
        pes.COD_EA as COD_EA,
        lr.LINKAGE_STEP as STEP
    FROM
        linked_record lr
        INNER JOIN linkage_pes_matching_var pes ON pes.ID_INDIV = lr.ID_INDIVIDUAL_PES
        INNER JOIN linkage_cens_matching_var cens ON cens.ID_INDIV = lr.ID_INDIVIDUAL_CENS;

