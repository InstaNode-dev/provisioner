package storage

import "testing"

func TestObjectPrefix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		token, provider, want string
	}{
		{"abcdefgh-0000-0000-0000-000000000001", "", "abcdefgh/"},
		{"ab", "", "ab/"},
		{"", "", ""},
		{"", "custom/", "custom/"},
		{"ignored", "my/prefix", "my/prefix/"},
		{"ignored", "my/prefix/", "my/prefix/"},
	}
	for _, tc := range cases {
		if got := objectPrefix(tc.token, tc.provider); got != tc.want {
			t.Errorf("objectPrefix(%q,%q) = %q, want %q", tc.token, tc.provider, got, tc.want)
		}
	}
}
