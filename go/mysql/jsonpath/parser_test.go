package jsonpath

import "testing"

func TestParserRoundtrip(t *testing.T) {
	for _, tc := range []struct {
		Input       string
		Expect      string
		ExpectError bool
	}{
		// Basic path expressions
		{Input: `$`, Expect: `$`},
		{Input: `$.foo`, Expect: `$."foo"`},
		{Input: `$[123]`, Expect: `$[123]`},
		{Input: `$[0]`, Expect: `$[0]`},

		// Multiple path elements
		{Input: `$.foo.bar`, Expect: `$."foo"."bar"`},
		{Input: `$.foo[0]`, Expect: `$."foo"[0]`},
		{Input: `$[0].foo`, Expect: `$[0]."foo"`},
		{Input: `$[0][1]`, Expect: `$[0][1]`},
		{Input: `$.a.b.c`, Expect: `$."a"."b"."c"`},
		{Input: `$[1][2][3]`, Expect: `$[1][2][3]`},
		{Input: `$.foo[0].bar[1]`, Expect: `$."foo"[0]."bar"[1]`},

		// Quoted strings with special characters
		{Input: `$."foo"`, Expect: `$."foo"`},
		{Input: `$."foo bar"`, Expect: `$."foo bar"`},
		{Input: `$."foo-bar"`, Expect: `$."foo-bar"`},
		{Input: `$."foo.bar"`, Expect: `$."foo.bar"`},
		{Input: `$."123"`, Expect: `$."123"`},
		{Input: `$."with spaces"`, Expect: `$."with spaces"`},
		{Input: `$.""`, Expect: `$.""`}, // Empty key name

		// Escaped characters in quoted strings
		{Input: `$."foo\"bar"`, Expect: `$."foo\"bar"`},
		{Input: `$."foo\\bar"`, Expect: `$."foo\\bar"`},
		{Input: `$."foo\nbar"`, Expect: `$."foo\nbar"`},
		{Input: `$."foo\tbar"`, Expect: `$."foo\tbar"`},
		{Input: `$."foo\rbar"`, Expect: `$."foo\rbar"`},
		{Input: `$."foo\bbar"`, Expect: `$."foo\bbar"`},
		{Input: `$."foo\fbar"`, Expect: `$."foo\fbar"`},

		// Unicode escape sequences
		{Input: `$."\u0041"`, Expect: `$."A"`}, // \u0041 = 'A'
		{Input: `$."\u00E9"`, Expect: `$."é"`}, // \u00E9 = 'é'
		{Input: `$."\u4E2D"`, Expect: `$."中"`}, // \u4E2D = '中' (Chinese character)
		{Input: `$."\u0041\u0042\u0043"`, Expect: `$."ABC"`},

		// ECMAScript identifiers
		{Input: `$.foo`, Expect: `$."foo"`},
		{Input: `$.FOO`, Expect: `$."FOO"`},
		{Input: `$.foo123`, Expect: `$."foo123"`},
		{Input: `$.foo_bar`, Expect: `$."foo_bar"`},
		{Input: `$.$foo`, Expect: `$."$foo"`},
		{Input: `$._foo`, Expect: `$."_foo"`},
		{Input: `$._`, Expect: `$."_"`},
		{Input: `$.$$`, Expect: `$."$$"`},
		{Input: `$.$_$`, Expect: `$."$_$"`},
		{Input: `$.foo$bar`, Expect: `$."foo$bar"`},

		// Large array indices
		{Input: `$[999999]`, Expect: `$[999999]`},
		{Input: `$[2147483647]`, Expect: `$[2147483647]`}, // Max int32

		// Complex nested paths
		{Input: `$.store.book[0].title`, Expect: `$."store"."book"[0]."title"`},
		{Input: `$.users[10].address.city`, Expect: `$."users"[10]."address"."city"`},
		{Input: `$[0][1][2].foo.bar[3]`, Expect: `$[0][1][2]."foo"."bar"[3]`},
		{Input: `$.a[0].b[1].c[2]`, Expect: `$."a"[0]."b"[1]."c"[2]`},

		// Mixed identifier and quoted string styles
		{Input: `$.foo."bar"`, Expect: `$."foo"."bar"`},
		{Input: `$."foo".bar`, Expect: `$."foo"."bar"`},
		{Input: `$.foo."bar baz"`, Expect: `$."foo"."bar baz"`},

		// Edge cases with quotes and escapes
		{Input: `$."\"\""`, Expect: `$."\"\""`},
		{Input: `$."\\\\"`, Expect: `$."\\\\"`},
		{Input: `$."\\\"\\\"\\\""`, Expect: `$."\\\"\\\"\\\""`},
		{Input: `$."a\"b\\c/d"`, Expect: `$."a\"b\\c/d"`},
	} {
		var result, err = Parse(tc.Input)
		if err != nil && !tc.ExpectError {
			t.Errorf("test case %q: %v", tc.Input, err)
		} else if tc.ExpectError && err == nil {
			t.Errorf("test case %q: no error when error was expected", tc.Input)
		} else if !tc.ExpectError && result.String() != tc.Expect {
			t.Errorf("round trip failure for %q: result %q does not match expected %q", tc.Input, result.String(), tc.Expect)
		}
	}
}
