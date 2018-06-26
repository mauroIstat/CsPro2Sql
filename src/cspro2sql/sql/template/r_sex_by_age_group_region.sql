CREATE OR REPLACE VIEW @SCHEMA.`aux_sex_age_region` AS
    SELECT 
        `q`.@QUESTIONNAIRE_COLUMN_FIRST_LEVEL_GEO AS `first_level_geo`,
        `h`.@INDIVIDUAL_COLUMN_SEX AS `sex`,
        `h`.@INDIVIDUAL_COLUMN_AGE AS `age`
    FROM
        (@SCHEMA.@INDIVIDUAL_TABLE `h`
        JOIN @SCHEMA.@QUESTIONNAIRE_TABLE `q` ON ((`h`.@QUESTIONNAIRE_COLUMN_BASE = `q`.`ID`)));

CREATE OR REPLACE VIEW @SCHEMA.`r_sex_by_age_group_region` AS
    SELECT 
        `a`.`first_level_geo` AS `region`,
        '0 to 4' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 0)
                AND (`aux_sex_age_region`.`age` < 5))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 0)
                AND (`aux_sex_age_region`.`age` < 5))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '5 to 9' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 5)
                AND (`aux_sex_age_region`.`age` < 10))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 5)
                AND (`aux_sex_age_region`.`age` < 10))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '10 to 14' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = 1)
                AND (`aux_sex_age_region`.`age` >= 10)
                AND (`aux_sex_age_region`.`age` < 15))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 10)
                AND (`aux_sex_age_region`.`age` < 15))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '15 to 19' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 15)
                AND (`aux_sex_age_region`.`age` < 20))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 15)
                AND (`aux_sex_age_region`.`age` < 20))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '20 to 24' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 20)
                AND (`aux_sex_age_region`.`age` < 25))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 20)
                AND (`aux_sex_age_region`.`age` < 25))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '25 to 29' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 25)
                AND (`aux_sex_age_region`.`age` < 30))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 25)
                AND (`aux_sex_age_region`.`age` < 30))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '30 to 34' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 30)
                AND (`aux_sex_age_region`.`age` < 35))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 30)
                AND (`aux_sex_age_region`.`age` < 35))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '35 to 39' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 35)
                AND (`aux_sex_age_region`.`age` < 40))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 35)
                AND (`aux_sex_age_region`.`age` < 40))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '40 to 44' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 40)
                AND (`aux_sex_age_region`.`age` < 45))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 40)
                AND (`aux_sex_age_region`.`age` < 45))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '45 to 49' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 45)
                AND (`aux_sex_age_region`.`age` < 50))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 45)
                AND (`aux_sex_age_region`.`age` < 50))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '50 to 54' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 50)
                AND (`aux_sex_age_region`.`age` < 55))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 50)
                AND (`aux_sex_age_region`.`age` < 55))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '55 to 59' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 55)
                AND (`aux_sex_age_region`.`age` < 60))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 55)
                AND (`aux_sex_age_region`.`age` < 60))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '60 to 64' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 60)
                AND (`aux_sex_age_region`.`age` < 65))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 60)
                AND (`aux_sex_age_region`.`age` < 65))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '65 to 69' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 65)
                AND (`aux_sex_age_region`.`age` < 70))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 65)
                AND (`aux_sex_age_region`.`age` < 70))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '70 to 74' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 70)
                AND (`aux_sex_age_region`.`age` < 75))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 70)
                AND (`aux_sex_age_region`.`age` < 75))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '75 to 79' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 75)
                AND (`aux_sex_age_region`.`age` < 80))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 75)
                AND (`aux_sex_age_region`.`age` < 80))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '80 to 84' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 80)
                AND (`aux_sex_age_region`.`age` < 85))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 80)
                AND (`aux_sex_age_region`.`age` < 85))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '85 to 89' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 85)
                AND (`aux_sex_age_region`.`age` < 90))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 85)
                AND (`aux_sex_age_region`.`age` < 90))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '90 to 94' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 90)
                AND (`aux_sex_age_region`.`age` < 95))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 90)
                AND (`aux_sex_age_region`.`age` < 95))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '95 to 97' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 95)
                AND (`aux_sex_age_region`.`age` < 97))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 95)
                AND (`aux_sex_age_region`.`age` < 97))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`))) 
    UNION SELECT 
        `a`.`first_level_geo` AS `region`,
        '97+' AS `range`,
        `a`.`male` AS `male`,
        `b`.`female` AS `female`
    FROM
        (((SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`, COUNT(0) AS `male`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex`  = @INDIVIDUAL_VALUE_SEX_MALE)
                AND (`aux_sex_age_region`.`age` >= 97))
        GROUP BY `aux_sex_age_region`.`first_level_geo`)) `a`
        JOIN (SELECT 
            `aux_sex_age_region`.`first_level_geo` AS `first_level_geo`,
                COUNT(0) AS `female`
        FROM
            @SCHEMA.`aux_sex_age_region`
        WHERE
            ((`aux_sex_age_region`.`sex` = @INDIVIDUAL_VALUE_SEX_FEMALE)
                AND (`aux_sex_age_region`.`age` >= 97))
        GROUP BY `aux_sex_age_region`.`first_level_geo`) `b` ON ((`a`.`first_level_geo` = `b`.`first_level_geo`)));
