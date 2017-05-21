DELIMITER ;;
CREATE FUNCTION @SCHEMA.`fix_dimension`(init_str varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare result varchar(100);
    if (length(init_str) > 3) then
        set result = concat(left(init_str, 2), right(init_str, 1));
    elseif (length(init_str) = 3) then
        set result = init_str;
    elseif (length(init_str) = 2) then
        set result = concat(init_str, "X");
    elseif (length(init_str) = 1) then
        set result = concat(init_str, "XX");
    elseif (length(init_str) = 0) then
        set result = "XXX";
    end if;
RETURN result;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`get_first_name`(theName varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare firstName varchar(100);
    if (locate(' ', theName) > 0) then
        set firstName = SUBSTRING_INDEX(theName, ' ', 1);
    else
        set firstName = theName;
    end if;
RETURN trim(firstName);
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`get_normalized_name`(theName varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare name varchar(100);
    declare start_result varchar(10);
    declare result varchar(100);

    set name = upper(trim(theName));
    set name = remove_duplicate(name);
    set name = replace(name, ' ', '');
    set name = remove_abbrev(name);
    set start_result = left(name, 1);

    if (start_result = 'I' or start_result = 'E')
        then set start_result = '1';
    elseif (start_result = 'O' or start_result = 'U')
        then set start_result = '2';
    end if;

    set result = right(name, length(name) - 1);
    set result = remove_not_admitted_char(result);
    set result = fix_dimension(result);

RETURN concat(start_result, result);
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`get_second_name`(theName varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare resultName varchar(100);
    declare firstName varchar(100);
    declare secondName varchar(100);
    set firstName = trim(get_first_name(theName));
    set secondName = trim(replace(theName, firstName, ''));
RETURN trim(get_first_name(secondName));
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`get_third_name`(theName varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare resultName varchar(100);
    declare partOne varchar(100);
    set partOne = concat(trim(get_first_name(theName)), get_second_name(theName));
    set resultName = replace(theName, ' ', '');
    if (length(resultName) > length(partOne)) then
        set resultName = trim(replace(resultName, partOne, ''));
    else
        set resultName = null;
    end if;
RETURN resultName;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`remove_abbrev`(init_str varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare result varchar(100);
    set result = init_str;
    set result = replace(result, 'W/', '');
    set result = replace(result, 'G/', '');
    set result = replace(result, 'H/', '');
    set result = replace(result, 'T/', '');
RETURN result;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`remove_duplicate`(the_text varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare result_text varchar(100);
    declare read_text varchar(100);

    set read_text = the_text;
    set result_text = '';

    while (length(read_text) > 0) do
        if (right(result_text, 1) <> left(read_text, 1)) then
            set result_text = concat(result_text, left(read_text, 1));
        end if;
        set read_text = substring(read_text from 2);
    end while;

RETURN result_text;
END ;;
DELIMITER ;

DELIMITER ;;
CREATE FUNCTION @SCHEMA.`remove_not_admitted_char`(init_str varchar(100)) RETURNS varchar(100) CHARSET utf8
BEGIN
    declare result varchar(100);
    set result = init_str;
    set result = replace(result, 'A', '');
    set result = replace(result, 'E', '');
    set result = replace(result, 'I', '');
    set result = replace(result, 'O', '');
    set result = replace(result, 'U', '');
    set result = replace(result, '/', '');
    set result = replace(result, ',', '');
    set result = replace(result, '.', '');
    set result = replace(result, ';', '');
    set result = replace(result, '1', '');
    set result = replace(result, '2', '');
    set result = replace(result, '3', '');
    set result = replace(result, '4', '');
    set result = replace(result, '5', '');
    set result = replace(result, '6', '');
    set result = replace(result, '7', '');
    set result = replace(result, '8', '');
    set result = replace(result, '9', '');
    set result = replace(result, '0', '');
    set result = replace(result, ' ', '');
RETURN result;
END ;;
DELIMITER ;

