
set global time_zone = "+00:00";

use drill_mysql_test;

create table person (
  person_id       INT NOT NULL AUTO_INCREMENT PRIMARY KEY,

  first_name      VARCHAR(255),
  last_name       VARCHAR(255),
  address         VARCHAR(255),
  city            VARCHAR(255),
  state           CHAR(2),
  zip             INT,

  json            VARCHAR(255),

  bigint_field    BIGINT,
  smallint_field  SMALLINT,
  numeric_field   NUMERIC(10, 2),
  boolean_field   BOOLEAN,
  double_field    DOUBLE,
  float_field     FLOAT,
  real_field      REAL,

  time_field      TIME,
  timestamp_field TIMESTAMP,
  date_field      DATE,
  datetime_field  DATETIME
)
engine=InnoDB default charset=latin1;

insert into person (first_name, last_name, address, city, state, zip, bigint_field, smallint_field, numeric_field,
                    boolean_field, double_field, float_field, real_field, time_field, timestamp_field, date_field, datetime_field, json)
    values ('first_name_1', 'last_name_1', '1401 John F Kennedy Blvd', 'Philadelphia', 'PA', 19107, 123456789, 1, 10.01,
            false, 1.0, 1.1, 1.2, '13:00:01', '2012-02-29 13:00:01', '2012-02-29', '2012-02-29 13:00:01', '{ a : 5, b : 6 }');

insert into person (first_name, last_name, address, city, state, zip, bigint_field, smallint_field, numeric_field,
                    boolean_field, double_field, float_field, real_field, time_field, timestamp_field, date_field, datetime_field, json)
    values ('first_name_2', 'last_name_2', 'One Ferry Building', 'San Francisco', 'CA', 94111, 95949393, 2, 20.03,
            true, 2.0, 2.1, 2.2, '23:59:59', '1999-09-09 23:59:59', '1999-09-09', '1999-09-09 23:59:59', '{ foo : "abc" }');

insert into person (first_name, last_name, address, city, state, zip, bigint_field, smallint_field, numeric_field,
                    boolean_field, double_field, float_field, real_field, time_field, timestamp_field, date_field, datetime_field, json)
    values ('first_name_3', 'last_name_3', '176 Bowery', 'New York', 'NY', 10012, 45456767, 3, 30.04,
            true, 3.0, 3.1, 3.2, '11:34:21', '2011-10-30 11:34:21', '2011-10-30', '2011-10-30 11:34:21', '{ z : [ 1, 2, 3 ] }');

insert into person (first_name, last_name, address, city, state, zip, json, bigint_field, smallint_field, numeric_field,
                    boolean_field, double_field, float_field, real_field, time_field, timestamp_field, date_field, datetime_field)
    values ('first_name_5', 'last_name_5', 'Chestnut Hill', 'Boston', 'MA', 12467, '{ [ a, b, c ] }', 123090, -3, 55.12, false, 5.0,
            5.1, 5.55, '16:00:01', '2015-06-02 10:01:01', '2015-06-01', '2015-09-22 15:46:10');

insert into person (person_id) values (5);
