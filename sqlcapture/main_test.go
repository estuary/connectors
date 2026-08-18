package sqlcapture

import "testing"

func TestResourceValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		res     Resource
		wantErr bool
	}{
		{
			name: "basic",
			res:  Resource{Namespace: "public", Stream: "foo"},
		},
		{
			name:    "invalid_mode",
			res:     Resource{Namespace: "public", Stream: "foo", Mode: "Bogus"},
			wantErr: true,
		},
		{
			name: "filter_with_automatic_mode",
			res: Resource{Namespace: "public", Stream: "foo",
				Advanced: &AdvancedResourceOptions{AdditionalBackfillFilter: "id > 123"}},
		},
		{
			name: "filter_with_normal_mode",
			res: Resource{Namespace: "public", Stream: "foo", Mode: BackfillModeNormal,
				Advanced: &AdvancedResourceOptions{AdditionalBackfillFilter: "id > 123"}},
		},
		{
			name: "filter_with_precise_mode",
			res: Resource{Namespace: "public", Stream: "foo", Mode: BackfillModePrecise,
				Advanced: &AdvancedResourceOptions{AdditionalBackfillFilter: "id > 123"}},
			wantErr: true,
		},
		{
			name: "precise_mode_without_filter",
			res:  Resource{Namespace: "public", Stream: "foo", Mode: BackfillModePrecise},
		},
		{
			name: "precise_mode_with_empty_advanced_options",
			res:  Resource{Namespace: "public", Stream: "foo", Mode: BackfillModePrecise, Advanced: &AdvancedResourceOptions{}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err = tc.res.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("expected an error but got none")
			} else if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
