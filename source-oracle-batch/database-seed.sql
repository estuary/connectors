CREATE TABLE test_all_types(
	id integer PRIMARY KEY,
	nvchar2 NVARCHAR2(4000),
	vchar2 VARCHAR2(4000),
	single_nchar NCHAR,
	vchar VARCHAR(4000),
	num NUMBER (38, 9),
	
	small_int SMALLINT,
	
	double_precision DOUBLE PRECISION,
	float_126 FLOAT(126),
	real_num REAL,
	
	datetime DATE,
	ts TIMESTAMP,
	ts_nine TIMESTAMP(9),
	
	ts_tz TIMESTAMP WITH TIME ZONE,
	ts_tz_nine TIMESTAMP(9) WITH TIME ZONE,
	
	ts_local_tz TIMESTAMP WITH LOCAL TIME ZONE,
	ts_local_tz_nine TIMESTAMP(9) WITH LOCAL TIME ZONE,

	interval_year INTERVAL YEAR(4) TO MONTH,
	interval_day INTERVAL DAY TO SECOND
)

INSERT INTO test_all_types(
	id,
	nvchar2,
	vchar2,
	single_nchar,
	vchar,
	num,
	small_int,
	double_precision,
	float_126,
	real_num,
	datetime,
	ts,
	ts_nine,
	ts_tz,
	ts_tz_nine,
	ts_local_tz,
	ts_local_tz_nine,
	interval_year,
	interval_day
) VALUES (
	1,
	'nvarchar2 value with unicode characters ❤️ 🔥️',
	'varchar2 value',
	'a',
	'varchar value',
	123456789.123456789,
	123456789,
	123456789.123456789,
	123456789.123456789,
	123456789.123456789,
	DATE '2022-01-01',
	TIMESTAMP '2022-01-01 13:00:00',
	TIMESTAMP '2022-01-01 14:00:00.123456789',
	TIMESTAMP '2022-01-01 15:00:00 +01:00',
	TIMESTAMP '2022-01-01 16:00:00.123456789 +01:00',
	TIMESTAMP '2022-01-01 17:00:00',
	TIMESTAMP '2022-01-01 18:00:00',
	INTERVAL '1234-5' YEAR(4) TO MONTH,
	INTERVAL '1 2:3:4.567' DAY TO SECOND(3)
)
