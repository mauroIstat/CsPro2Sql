CREATE VIEW `r_questionnaire_info` AS
    SELECT 
        COUNT(0) AS `total`,
        `avg_individual`.`avg_individual` AS `avg_individual`,
        `avg_individual_male`.`avg_individual_male` AS `avg_individual_male`,
        `avg_individual_female`.`avg_individual_female` AS `avg_individual_female`
    FROM
        (((@SCHEMA.@QUESTIONNAIRE_TABLE
        JOIN (SELECT 
            AVG(`a`.`num`) AS `avg_individual`
        FROM
            (SELECT 
            COUNT(0) AS `num`
        FROM
            @SCHEMA.@INDIVIDUAL_TABLE
        GROUP BY @SCHEMA.@INDIVIDUAL_TABLE.@QUESTIONNAIRE_COLUMN) `a`) `avg_individual`)
        JOIN (SELECT 
            AVG(`a`.`num`) AS `avg_individual_male`
        FROM
            (SELECT 
            COUNT(0) AS `num`
        FROM
            @SCHEMA.@INDIVIDUAL_TABLE
        WHERE
            (@SCHEMA.@INDIVIDUAL_TABLE.@INDIVIDUAL_COLUMN_SEX = @INDIVIDUAL_VALUE_SEX_MALE)
        GROUP BY @SCHEMA.@INDIVIDUAL_TABLE.@QUESTIONNAIRE_COLUMN) `a`) `avg_individual_male`)
        JOIN (SELECT 
            AVG(`a`.`num`) AS `avg_individual_female`
        FROM
            (SELECT 
            COUNT(0) AS `num`
        FROM
            @SCHEMA.@INDIVIDUAL_TABLE
        WHERE
            (@SCHEMA.@INDIVIDUAL_TABLE.@INDIVIDUAL_COLUMN_SEX = @INDIVIDUAL_VALUE_SEX_FEMALE)
        GROUP BY @SCHEMA.@INDIVIDUAL_TABLE.@QUESTIONNAIRE_COLUMN) `a`) `avg_individual_female`);

