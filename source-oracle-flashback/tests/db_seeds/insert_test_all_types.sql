INSERT INTO flow_test.test_all_types(
	id,
	nvchar2,
	vchar2,
	single_nchar,
	vchar,
	num,
	small_int,
  integ,
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
	interval_day,
	r
) VALUES (
	1,
	'nvarchar2 value with unicode characters ❤️ 🔥️',
	'varchar2 value',
	'a',
	'varchar value',
	123456789.123456789,
	123456789,
  18446744073709551615,
	123456789.123456789,
	123456789.123456789,
	123456789.123456789,
	DATE '2022-01-01',
	TIMESTAMP '2022-01-01 13:00:00',
	TIMESTAMP '2022-01-01 13:00:00.123456789',
	TIMESTAMP '2022-01-01 14:00:00 +01:00',
	TIMESTAMP '2022-01-01 14:00:00.123456789 +01:00',
	TIMESTAMP '2022-01-01 15:00:00 +02:00',
	TIMESTAMP '2022-01-01 15:00:00.123456789 +02:00',
	INTERVAL '1234-5' YEAR(4) TO MONTH,
	INTERVAL '1 2:3:4.567' DAY TO SECOND(3),
	UTL_RAW.CAST_TO_RAW('testing raw value')
)
