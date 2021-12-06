#SCHEMA DEFINITION------------------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS dm_covid;
CREATE SCHEMA dm_covid;
USE dm_covid;


#TABLE DEFINITIONS------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS d_region;
CREATE TABLE d_region(
    sk_region INT NOT NULL,
    bk_province_state INT NOT NULL,
    province_state VARCHAR(100) NOT NULL,
    bk_country_region INT NOT NULL,
    country_region VARCHAR(100) NOT NULL,
    lat FLOAT NOT NULL,
    `long` FLOAT NOT NULL
);


DROP TABLE IF EXISTS d_date;
CREATE TABLE d_date(
    sk_date INT NOT NULL,
    date DATE NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_of_week_name VARCHAR(10) NOT NULL,
    week_of_year INT NOT NULL,
    sk_month INT NOT NULL,
    month_of_year INT NOT NULL,
    month_of_year_name VARCHAR(10) NOT NULL,
    sk_quarter INT NOT NULL,
    quarter_of_year INT NOT NULL,
    sk_semester INT NOT NULL,
    semester_of_year INT NOT NULL,
    year INT NOT NULL
);


DROP TABLE IF EXISTS f_covid;
CREATE TABLE f_covid(
    sk_region INT NOT NULL,
    sk_date INT NOT NULL,
    confirmed_cases INT NOT NULL,
    death_cases INT NOT NULL,
    recovered_cases INT NOT NULL
);



#DATE DIM SP INSERT-----------------------------------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS InsertCalendar;
DELIMITER //
CREATE PROCEDURE InsertCalendar(dt DATE)
    BEGIN
       INSERT INTO d_date
       VALUES(
          CONCAT(EXTRACT(YEAR FROM dt), RIGHT(CONCAT('0', EXTRACT(MONTH FROM dt)),2), RIGHT(CONCAT('0', EXTRACT(DAY FROM dt)), 2))
        , dt
        , DAYOFMONTH(dt)
        , DAYOFWEEK(dt)
        , DAYNAME(dt)
        , EXTRACT(WEEK FROM dt)
        , CONCAT(EXTRACT(YEAR FROM dt), RIGHT(CONCAT('0', EXTRACT(MONTH FROM dt)),2))
        , EXTRACT(MONTH FROM dt)
        , MONTHNAME(dt)
        , CONCAT(EXTRACT(YEAR FROM dt), RIGHT(CONCAT('0', EXTRACT(QUARTER FROM dt)),2))
        , EXTRACT(QUARTER FROM dt)
        , IF(EXTRACT(QUARTER FROM dt) < 3, CAST(CONCAT(EXTRACT(YEAR FROM @dt), '01') AS SIGNED),
             CAST(CONCAT(EXTRACT(YEAR FROM dt), '02') AS SIGNED))
        , IF(EXTRACT(QUARTER FROM dt) < 3, 1, 2)
        , EXTRACT(YEAR FROM dt)
      );
    END //
DELIMITER ;


DROP PROCEDURE IF EXISTS GenerateCalendar;
DELIMITER //
CREATE PROCEDURE GenerateCalendar(
      IN date_start date
    , IN date_end date
)
BEGIN
    WHILE date_start <= date_end DO
        CALL InsertCalendar(date_start);
        SET date_start = DATE_ADD(date_start, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;



#TEST InsertCalendar----------------------------------------------------------------------------------------------------
SET @dt = CAST('2021-12-05' as date);
SELECT
      CONCAT(EXTRACT(YEAR FROM @dt), RIGHT(CONCAT('0', EXTRACT(MONTH FROM @dt)),2), RIGHT(CONCAT('0', EXTRACT(DAY FROM @DT)), 2))
    , @dt
    , DAYOFMONTH(@dt)
    , DAYOFWEEK(@dt)
    , DAYNAME(@dt)
    , EXTRACT(WEEK FROM @dt)
    , CONCAT(EXTRACT(YEAR FROM @dt), RIGHT(CONCAT('0', EXTRACT(MONTH FROM @dt)),2))
    , EXTRACT(MONTH FROM @dt)
    , MONTHNAME(@dt)
    , CONCAT(EXTRACT(YEAR FROM @dt), RIGHT(CONCAT('0', EXTRACT(QUARTER FROM @dt)),2))
    , EXTRACT(QUARTER FROM @dt)
    , IF(EXTRACT(QUARTER FROM @dt) < 3, CAST(CONCAT(EXTRACT(YEAR FROM @dt), '01') AS SIGNED),
         CAST(CONCAT(EXTRACT(YEAR FROM @dt), '02') AS SIGNED))
    , IF(EXTRACT(QUARTER FROM @dt) < 3, 1, 2)
    , EXTRACT(YEAR FROM @dt);


#RUN GenerateCalendar---------------------------------------------------------------------------------------------------
SET @date_start = CAST('2019-01-01' as date);
SET @date_end = CAST('2022-12-31' as date);
CALL GenerateCalendar(@date_start, @date_end)

SELECT * FROM d_date LIMIT 1000;
SELECT year, count(distinct date)
FROM d_date
GROUP BY year;