package hubspot

var (
	// companiesProperties contains some of the default property data for the
	// Companies object.
	companiesProperties = map[string]*Property{
		"domain": {
			Name: "domain",
			Type: StringPropertyType,
		},
		"name": {
			Name: "name",
			Type: StringPropertyType,
		},
		"city": {
			Name: "city",
			Type: StringPropertyType,
		},
		"phone": {
			Name:      "phone",
			Type:      StringPropertyType,
			FieldType: "phonenumber",
		},
	}

	// contactsProperties contains some of the default property data for the
	// Contacts object.  This object has a unique property that makes it ideal
	// for testing the unique property path.
	contactsProperties = map[string]*Property{
		"email": {
			Name:           "email",
			Type:           StringPropertyType,
			HasUniqueValue: true,
		},
		"firstname": {
			Name: "firstname",
			Type: StringPropertyType,
		},
		"lastname": {
			Name: "lastname",
			Type: StringPropertyType,
		},
		"city": {
			Name: "city",
			Type: StringPropertyType,
		},
		"company": {
			Name: "company",
			Type: StringPropertyType,
		},
		"hs_lead_status": {
			Name: "hs_lead_status",
			Type: EnumPropertyType,
			Options: []PropertyOption{
				{Value: "NEW"},
				{Value: "OPEN"},
				{Value: "IN_PROGRESS"},
				{Value: "OPEN_DEAL"},
				{Value: "UNQUALIFIED"},
				{Value: "ATTEMPT_TO_CONTACT"},
				{Value: "CONNECTED"},
				{Value: "BAD_TIMING"},
			},
		},
		"followercount": {
			Name: "followercount",
			Type: NumberPropertyType,
		},
		"hs_cross_sell_opportunity": {
			Name: "hs_cross_sell_opportunity",
			Type: BoolPropertyType,
		},
		"hs_job_change_detected_date": {
			Name: "hs_job_change_detected_date",
			Type: DatePropertyType,
		},
		"closedate": {
			Name: "closedate",
			Type: DatetimePropertyType,
		},
	}
)
