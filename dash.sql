USE dashboard;

CREATE TABLE dashboard.`c_user` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `FIRSTNAME` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `MIDDLENAME` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `LASTNAME` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `EMAIL` varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL,
  `PASSWORD` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `EMAIL_UNIQUE` (`EMAIL`(255)),
  KEY `EMAIL_INDEX` (`EMAIL`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dashboard.`c_user` (FIRSTNAME, LASTNAME, EMAIL, PASSWORD)
  VALUES ('Dashboard', 'Admin', 'admin@dashboard', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918');
INSERT INTO dashboard.`c_user` (FIRSTNAME, LASTNAME, EMAIL, PASSWORD)
  VALUES ('Dashboard', 'Guest', 'guest@dashboard', '84983c60f7daadc1cb8698621f802c0d9f9a3c3c295c810748fb048115c186ec');
CREATE TABLE dashboard.`c_user_roles` (
  `EMAIL` varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL,
  `ROLE` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`EMAIL`,`ROLE`),
  CONSTRAINT `c_user_roles_ibfk_1` FOREIGN KEY (`EMAIL`) REFERENCES `c_user` (`EMAIL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO dashboard.`c_user_roles` VALUES ('admin@dashboard', 'ADMIN');
INSERT INTO dashboard.`c_user_roles` VALUES ('guest@dashboard', 'GUEST');
CREATE TABLE dashboard.`cspro2sql_stats` (
  `NAME` varchar(256) NOT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE VIEW dashboard.`r_questionnaire_info` AS
    SELECT 
        COUNT(0) AS `total`,
        `avg_individual`.`avg_individual` AS `avg_individual`,
        `avg_individual_male`.`avg_individual_male` AS `avg_individual_male`,
        `avg_individual_female`.`avg_individual_female` AS `avg_individual_female`
    FROM
        (((dashboard.H_HOUSEHOLD_QUEST
        JOIN (SELECT 
            AVG(`a`.`num`) AS `avg_individual`
        FROM
            (SELECT 
            COUNT(0) AS `num`
        FROM
            dashboard.H_INDIVIDUAL
        GROUP BY dashboard.H_INDIVIDUAL.HOUSEHOLD_QUEST) `a`) `avg_individual`)
        JOIN (SELECT 
            AVG(`a`.`num`) AS `avg_individual_male`
        FROM
            (SELECT 
            COUNT(0) AS `num`
        FROM
            dashboard.H_INDIVIDUAL
        WHERE
            (dashboard.H_INDIVIDUAL.P307 = 1)
        GROUP BY dashboard.H_INDIVIDUAL.HOUSEHOLD_QUEST) `a`) `avg_individual_male`)
        JOIN (SELECT 
            AVG(`a`.`num`) AS `avg_individual_female`
        FROM
            (SELECT 
            COUNT(0) AS `num`
        FROM
            dashboard.H_INDIVIDUAL
        WHERE
            (dashboard.H_INDIVIDUAL.P307 = 2)
        GROUP BY dashboard.H_INDIVIDUAL.HOUSEHOLD_QUEST) `a`) `avg_individual_female`);

SELECT @ID := 0;
CREATE TABLE dashboard.mr_questionnaire_info (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_questionnaire_info.* FROM dashboard.r_questionnaire_info;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_questionnaire_info');

CREATE VIEW dashboard.`r_individual_info` AS
    SELECT 
        `total`.`total` AS `total`,
        `total`.`age_avg` AS `age_avg`,
        `total`.`age_max` AS `age_max`,
        `male`.`total_male` AS `total_male`,
        `male`.`age_male_avg` AS `age_avg_male`,
        `male`.`age_male_max` AS `age_max_male`,
        `female`.`total_female` AS `total_female`,
        `female`.`age_female_avg` AS `age_avg_female`,
        `female`.`age_female_max` AS `age_max_female`
    FROM
        ((((SELECT 
            COUNT(0) AS `total`,
                AVG(dashboard.H_INDIVIDUAL.P308) AS `age_avg`,
                MAX(dashboard.H_INDIVIDUAL.P308) AS `age_max`
        FROM
            dashboard.H_INDIVIDUAL)) `total`
        JOIN (SELECT 
            COUNT(0) AS `total_male`,
                AVG(dashboard.H_INDIVIDUAL.P308) AS `age_male_avg`,
                MAX(dashboard.H_INDIVIDUAL.P308) AS `age_male_max`
        FROM
            dashboard.H_INDIVIDUAL
        WHERE
            (dashboard.H_INDIVIDUAL.P307 = 1)) `male`)
        JOIN (SELECT 
            COUNT(0) AS `total_female`,
                AVG(dashboard.H_INDIVIDUAL.P308) AS `age_female_avg`,
                MAX(dashboard.H_INDIVIDUAL.P308) AS `age_female_max`
        FROM
            dashboard.H_INDIVIDUAL
        WHERE
            (dashboard.H_INDIVIDUAL.P307 = 2)) `female`);

SELECT @ID := 0;
CREATE TABLE dashboard.mr_individual_info (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_individual_info.* FROM dashboard.r_individual_info;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_individual_info');

CREATE VIEW dashboard.`r_religion` AS
    SELECT 
        `vs`.`VALUE` AS `RELIGION`, COUNT(0) AS `INDIVIDUALS`
    FROM
        (dashboard.H_INDIVIDUAL `i`
        JOIN dashboard.VSH_P310 `vs` ON ((`i`.P310 = `vs`.`ID`)))
    GROUP BY `vs`.`VALUE`;

SELECT @ID := 0;
CREATE TABLE dashboard.mr_religion (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_religion.* FROM dashboard.r_religion;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_religion');

CREATE VIEW dashboard.`r_sex_by_age` AS
    SELECT 
        `a`.`p308` AS `age`,
        `a`.`total` AS `total`,
        `b`.`total_male` AS `total_male`,
        `c`.`total_female` AS `total_female`
    FROM
        (((SELECT 
            dashboard.H_INDIVIDUAL.P308 AS `p308`,
                COUNT(0) AS `total`
        FROM
            dashboard.H_INDIVIDUAL
        GROUP BY dashboard.H_INDIVIDUAL.P308) `a`
        JOIN (SELECT 
            dashboard.H_INDIVIDUAL.P308 AS `p308`,
                COUNT(0) AS `total_male`
        FROM
            dashboard.H_INDIVIDUAL
        WHERE
            (dashboard.H_INDIVIDUAL.P307 = 1)
        GROUP BY dashboard.H_INDIVIDUAL.P308) `b`)
        JOIN (SELECT 
            dashboard.H_INDIVIDUAL.P308 AS `p308`,
                COUNT(0) AS `total_female`
        FROM
            dashboard.H_INDIVIDUAL
        WHERE
            (dashboard.H_INDIVIDUAL.P307 = 2)
        GROUP BY dashboard.H_INDIVIDUAL.P308) `c`)
    WHERE
        ((`a`.`p308` = `b`.`p308`)
            AND (`b`.`p308` = `c`.`p308`));

SELECT @ID := 0;
CREATE TABLE dashboard.mr_sex_by_age (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_sex_by_age.* FROM dashboard.r_sex_by_age;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_sex_by_age');

CREATE VIEW dashboard.`r_sex_by_region` AS
    SELECT 
        `region`.`VALUE` AS `REGION`,
        `sex`.`VALUE` AS `SEX`,
        COUNT(0) AS `INDIVIDUALS`
    FROM
        (((dashboard.H_HOUSEHOLD_QUEST `h`
        JOIN VSH_P304A `region` ON ((`h`.ID101 = `region`.`ID`)))
        JOIN dashboard.H_INDIVIDUAL `i` ON ((`h`.`ID` = `i`.HOUSEHOLD_QUEST)))
        JOIN VSH_P307 `sex` ON ((`i`.P307 = `sex`.`ID`)))
    GROUP BY `region`.`VALUE` , `sex`.`VALUE`;

SELECT @ID := 0;
CREATE TABLE dashboard.mr_sex_by_region (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_sex_by_region.* FROM dashboard.r_sex_by_region;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_sex_by_region');

CREATE VIEW dashboard.`r_regional_area` AS
  SELECT 'Region' name, COUNT(0) value FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101) a0 UNION
  SELECT 'Zone', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102) a1 UNION
  SELECT 'Woreda', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103) a2 UNION
  SELECT 'City', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103,ID104) a3 UNION
  SELECT 'Subcity', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103,ID104,ID105) a4 UNION
  SELECT 'Psa', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103,ID104,ID105,ID106) a5 UNION
  SELECT 'Sa', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103,ID104,ID105,ID106,ID107) a6 UNION
  SELECT 'Kebele', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103,ID104,ID105,ID106,ID107,ID108) a7 UNION
  SELECT 'EA', COUNT(0) FROM (SELECT COUNT(0) FROM dashboard.H_HOUSEHOLD_QUEST GROUP BY ID101,ID102,ID103,ID104,ID105,ID106,ID107,ID108,ID109) a8
;
SELECT @ID := 0;
CREATE TABLE dashboard.mr_regional_area (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_regional_area.* FROM dashboard.r_regional_area;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_regional_area');

CREATE VIEW dashboard.`r_sex_by_age_group` AS
  SELECT '0 to 4' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 0 AND P308 < 5) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 0 AND P308 < 5) b UNION
  SELECT '5 to 10' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 5 AND P308 < 11) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 5 AND P308 < 11) b UNION
  SELECT '11 to 16' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 11 AND P308 < 17) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 11 AND P308 < 17) b UNION
  SELECT '17 to 25' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 17 AND P308 < 26) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 17 AND P308 < 26) b UNION
  SELECT '26 to 34' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 26 AND P308 < 35) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 26 AND P308 < 35) b UNION
  SELECT '35 to 45' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 35 AND P308 < 46) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 35 AND P308 < 46) b UNION
  SELECT '46 to 55' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 46 AND P308 < 56) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 46 AND P308 < 56) b UNION
  SELECT '56 to 65' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 56 AND P308 < 66) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 56 AND P308 < 66) b UNION
  SELECT '66 to 78' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 66 AND P308 < 79) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 66 AND P308 < 79) b UNION
  SELECT '79 to 99' as 'range', a.male, b.female FROM (SELECT COUNT(0) male FROM dashboard.H_INDIVIDUAL WHERE P307 = 1 AND P308 >= 79 AND P308 < 100) a,(SELECT COUNT(0) female FROM dashboard.H_INDIVIDUAL WHERE P307 = 2 AND P308 >= 79 AND P308 < 100) b
;
SELECT @ID := 0;
CREATE TABLE dashboard.mr_sex_by_age_group (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_sex_by_age_group.* FROM dashboard.r_sex_by_age_group;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_sex_by_age_group');

CREATE VIEW dashboard.`r_household_by_ea` AS
  SELECT concat('Region','#','Zone','#','Woreda','#','City','#','Subcity','#','Psa','#','Sa','#','Kebele','#','EA') as name, null as household
  UNION
  SELECT concat(vs0.value,'#',h.ID102,'#',h.ID103,'#',h.ID104,'#',h.ID105,'#',h.ID106,'#',h.ID107,'#',h.ID108,'#',h.ID109) as name, COUNT(0) AS `household`
  FROM dashboard.H_HOUSEHOLD_QUEST `h`
    JOIN dashboard.VSH_P304A vs0 ON `h`.`ID101` = vs0.`ID`
  GROUP BY `h`.ID101, `h`.ID102, `h`.ID103, `h`.ID104, `h`.ID105, `h`.ID106, `h`.ID107, `h`.ID108, `h`.ID109;
SELECT @ID := 0;
CREATE TABLE dashboard.mr_household_by_ea (PRIMARY KEY (ID)) AS SELECT @ID := @ID + 1 ID, r_household_by_ea.* FROM dashboard.r_household_by_ea;
INSERT INTO dashboard.`cspro2sql_stats` VALUES ('r_household_by_ea');

