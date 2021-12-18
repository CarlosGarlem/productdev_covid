CREATE TABLE dm_covid.covid(
    id int primary key,
    name varchar(100)
);


#USE SCHEMA-------------------------------------------------------------------------------------------------------------
USE dm_covid;

#TABLE DEFINITIONS------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS landing_deaths;
create table landing_deaths
(
	province_state varchar(100) null,
	country_region varchar(100) null,
	lat float null,
	`long` float null,
	`date` date null,
	delta int null
);


DROP TABLE IF EXISTS landing_recovered;
create table landing_recovered
(
	province_state varchar(100) null,
	country_region varchar(100) null,
	lat float null,
	`long` float null,
	`date` date null,
	delta int null
);


DROP TABLE IF EXISTS landing_confimed;
create table landing_confimed
(
	province_state varchar(100) null,
	country_region varchar(100) null,
	lat float null,
	`long` float null,
	`date` date null,
	delta int null
);


DROP TABLE IF EXISTS d_region;
CREATE TABLE d_region(
    sk_region serial,
    province_state VARCHAR(100) NOT NULL,
    country_region VARCHAR(100) NOT NULL,
    lat FLOAT NULL,
    `long` FLOAT NULL
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


DROP VIEW IF EXISTS covid_view;
CREATE VIEW covid_view AS
SELECT d_date.date
     , d_region.country_region
     , d_region.province_state
     , d_region.lat
     , d_region.`long`
     , IF(f_covid.confirmed_cases < 0, 0, f_covid.confirmed_cases)      as confirmed_cases
     , IF(f_covid.death_cases < 0, 0, f_covid.death_cases)              as death_cases
     , IF(f_covid.recovered_cases < 0, 0, f_covid.recovered_cases)      as recovered_cases
FROM f_covid
LEFT OUTER JOIN d_region
ON f_covid.sk_region = d_region.sk_region
LEFT OUTER JOIN d_date
ON f_covid.sk_date = d_date.sk_date;


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
        , IF(EXTRACT(QUARTER FROM dt) < 3, CAST(CONCAT(EXTRACT(YEAR FROM dt), '01') AS SIGNED),
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