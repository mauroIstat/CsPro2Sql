CREATE TABLE @SCHEMA.`c_user_roles` (
  `EMAIL` varchar(256) COLLATE utf8mb4_unicode_ci NOT NULL,
  `ROLE` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`EMAIL`,`ROLE`),
  CONSTRAINT `c_user_roles_ibfk_1` FOREIGN KEY (`EMAIL`) REFERENCES `c_user` (`EMAIL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO @SCHEMA.`c_user_roles` VALUES ('admin@dashboard', 'ADMIN');
INSERT INTO @SCHEMA.`c_user_roles` VALUES ('guest@dashboard', 'GUEST');
