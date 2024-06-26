package main

import "testing"

func TestIgnoreQueries(t *testing.T) {
	var cases = map[string]bool{
		`BEGIN`:                     true,
		`COMMIT`:                    true,
		`CREATE DEFINER`:            true,
		`CREATE OR REPLACE DEFINER`: true,
		`CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER`: true,
		`CREATE ALGORITHM = TEMPTABLE DEFINER`:          true,

		`CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret1234'`: true,

		"# This is a comment\n ALTER TABLE foobar ADD COLUMN x INTEGER;":        false,
		`/* This is also a comment */ ALTER TABLE foobar ADD COLUMN x INTEGER;`: false,

		`CREATE DATABASE IF NOT EXISTS test`:     false,
		`INSERT INTO foobar VALUES (1, 'hello')`: false,
		`DROP TABLE foobar`:                      false,
	}
	for input, expect := range cases {
		if ignoreQueriesRe.MatchString(input) != expect {
			t.Errorf("ignore result mismath for %q (expected %v)", input, expect)
		}
	}
}
