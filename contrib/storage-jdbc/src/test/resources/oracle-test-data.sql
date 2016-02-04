Create sequence id_sequencess
start with 1
NOMAXVALUE;

create table person (
  person_id       NUMBER(10) NOT NULL,

  first_name      VARCHAR(255),
  last_name       VARCHAR(255),
  address         VARCHAR(255),
  city            VARCHAR(255),
  state_field     CHAR(2),
  zip             INT,

  json_field      VARCHAR(255),

  smallint_field  SMALLINT,
  numeric_field   NUMERIC(10, 2),
  boolean_field   NUMBER(2),
  double_field    DOUBLE PRECISION,
  float_field     FLOAT,
  real_field      REAL,

  timestamp_field TIMESTAMP,
  date_field      DATE,



  text_field      VARCHAR2(220),
  nchar_field     NCHAR,
  blob_field      BLOB,
  clob_field      CLOB,

  decimal_field   DECIMAL,
  dec_field       DEC,

  constraint pk_person_id PRIMARY KEY(person_id)

);


insert into person (person_id, first_name, last_name, address, city, state_field, zip, json_field,
                    smallint_field, numeric_field, boolean_field, double_field, float_field, real_field,
                    timestamp_field, date_field, text_field, nchar_field, blob_field,
                    clob_field, decimal_field, dec_field)
       values(id_sequencess.nextval,'first_name_1', 'last_name_1', '1401 John F Kennedy Blvd', 'Philadelphia', 'PA', 19107, '{ a : 5, b : 6 }',
              10.01, 123, 0, 1.340, 1.112, 1.224, TO_TIMESTAMP('2011/02/11 23:12:12','YYYY/MM/DD HH24:MI:SS'), TO_DATE('2015/05/2', 'yyyy/mm/dd'),
              'It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout',
              '1', utl_raw.cast_to_raw('this is test'), rpad('n',5,'-'), 123.25, 0.5
       );


insert into person (person_id, first_name, last_name, address, city, state_field, zip, json_field,
                    smallint_field, numeric_field, boolean_field, double_field, float_field, real_field,
                    timestamp_field, date_field, text_field, nchar_field, blob_field,
                    clob_field, decimal_field, dec_field)
       values(id_sequencess.nextval,'first_name_2', 'last_name_2', 'One Ferry Building', 'San Francisco', 'CA', 94111, '{ name : Sem, surname : Sem }',
              4.21, 45, 1, 2.540, 4.242, 4.252, TO_TIMESTAMP('2015/12/20 23:12:12','YYYY/MM/DD HH24:MI:SS'), TO_DATE('2015/12/1', 'yyyy/mm/dd'), 'Some text', '1', utl_raw.cast_to_raw('this is test2'), rpad('n',5,'!'), 13.656, 0.1
       );


insert into person (person_id, first_name, last_name, address, city, state_field, zip, json_field,
                    smallint_field, numeric_field, boolean_field, double_field, float_field, real_field,
                    timestamp_field, date_field, text_field, nchar_field, blob_field,
                    clob_field, decimal_field, dec_field)
       values(id_sequencess.nextval,'first_name_3', 'last_name_3', '176 Bowery', 'New York', 'NY', 10012, '{ x : 12, y : 16 }',
              0.41, 13, 0, 41.42, 2.421, 6.12, TO_TIMESTAMP('1901/01/01 23:12:12','YYYY/MM/DD HH24:MI:SS'), TO_DATE('2015/05/2 12:12:30', 'yyyy/mm/dd hh:mi:ss'), 'Some long text 3',
              '0', utl_raw.cast_to_raw('this is test3'), rpad('n',5,'?'), 0.456, 1.0
       );

insert into person (person_id) values(5);

