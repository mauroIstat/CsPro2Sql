DROP TABLE IF EXISTS @SCHEMA.linked_record;

CREATE TABLE @SCHEMA.`linked_record` (
  `ID_INDIVIDUAL_PES` varchar(45) NOT NULL,
  `ID_QUEST_PES` varchar(45) NOT NULL,
  `ID_INDIVIDUAL_CENS` varchar(45) NOT NULL,
  `ID_QUEST_CENS` varchar(45) NOT NULL,
  `DATA` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `LINKAGE_STEP` varchar(45) NOT NULL DEFAULT,
  PRIMARY KEY (`ID_INDIVIDUAL_PES`,`ID_INDIVIDUAL_CENS`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO @SCHEMA.linked_record (ID_INDIVIDUAL_PES, ID_QUEST_PES, ID_INDIVIDUAL_CENS, ID_QUEST_CENS, LINKAGE_STEP)
(
    SELECT
        pes.ID_INDIV AS ID_INDIVIDUAL_PES,
        pes.ID_QUEST AS ID_QUEST_PES,
        cens.ID_INDIV AS ID_INDIVIDUAL_CENS,
        cens.ID_QUEST AS ID_QUEST_CENS,
        '0'
    FROM linkage_pes_matching_var pes INNER JOIN linkage_cens_matching_var cens ON
        pes.NAME1 = cens.NAME1 AND
        pes.NAME2 = cens.NAME2 AND
        pes.NAME3 = cens.NAME3 AND
        pes.SEX = cens.SEX AND
        pes.RELIGION = cens.RELIGION AND
        pes.RELAT = cens.RELAT AND
        pes.GRADE = cens.GRADE AND
        pes.TONGUE = cens.TONGUE AND
        pes.AGE = cens.AGE AND
        pes.MARITAL = cens.MARITAL AND
        pes.COD_EA = cens.COD_EA
);

