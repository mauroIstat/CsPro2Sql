CREATE OR REPLACE VIEW @SCHEMA.`r_sex_by_region` AS
    SELECT 
        `region`.`VALUE` AS `REGION`,
        `sex`.`VALUE` AS `SEX`,
        COUNT(0) AS `INDIVIDUALS`
    FROM
        (((@SCHEMA.@QUESTIONNAIRE_TABLE `h`
        JOIN @VALUESET_REGION `region` ON ((`h`.@QUESTIONNAIRE_COLUMN_REGION = `region`.`ID`)))
        JOIN @SCHEMA.@INDIVIDUAL_TABLE `i` ON ((`h`.`ID` = `i`.@QUESTIONNAIRE_COLUMN_BASE)))
        JOIN @VALUESET_SEX `sex` ON ((`i`.@INDIVIDUAL_COLUMN_SEX = `sex`.`ID`)))
    GROUP BY `region`.`VALUE` , `sex`.`VALUE`;

