package hubspot

import (
	"fmt"
	"strings"
)

// CRMObject are the object types the connector supports.
type CRMObject string

// ObjectName is the name of the type.  This is the same as CRMObject except in
// the case of Custom Objects.  A user will know this value for a Custom Object.
type ObjectName string

// ObjectTypeID is a unique identifier that can be used in API calls.  This can
// be a string like most of the CRMObjects for built-in types, or an ID like
// `2-270964404`.  This value is typically unknown by the user.
type ObjectTypeID string

const (
	CallsObject        CRMObject = "calls"
	CompaniesObject    CRMObject = "companies"
	ContactsObject     CRMObject = "contacts"
	DealsObject        CRMObject = "deals"
	EmailsObject       CRMObject = "emails"
	LineItemsObject    CRMObject = "line_items"
	MeetingsObject     CRMObject = "meetings"
	PostalMailObject   CRMObject = "postal_mail"
	ProductsObject     CRMObject = "products"
	QuotesObject       CRMObject = "quotes"
	TasksObject        CRMObject = "tasks"
	TicketsObject      CRMObject = "tickets"
	CustomObjectObject CRMObject = "custom_object"
	UnknownObject      CRMObject = "unknown"
)

var (
	allCRMObjects = []CRMObject{
		CallsObject,
		CompaniesObject,
		ContactsObject,
		DealsObject,
		EmailsObject,
		LineItemsObject,
		MeetingsObject,
		PostalMailObject,
		ProductsObject,
		QuotesObject,
		TasksObject,
		TicketsObject,
		CustomObjectObject,
	}
)

// NewCRMObject creates a CRMObject from an API string.
func NewCRMObject(s string) (CRMObject, error) {
	switch s {
	case "calls":
		return CallsObject, nil
	case "companies":
		return CompaniesObject, nil
	case "contacts":
		return ContactsObject, nil
	case "deals":
		return DealsObject, nil
	case "emails":
		return EmailsObject, nil
	case "line_items":
		return LineItemsObject, nil
	case "meetings":
		return MeetingsObject, nil
	case "postal_mail":
		return PostalMailObject, nil
	case "products":
		return ProductsObject, nil
	case "quotes":
		return QuotesObject, nil
	case "tasks":
		return TasksObject, nil
	case "tickets":
		return TicketsObject, nil
	case "custom_object":
		return CustomObjectObject, nil
	default:
		return UnknownObject, fmt.Errorf("unknown object type: %q", s)
	}
}

// String is the value to use for API calls and other storage such as the
// resource path.
func (o CRMObject) String() string {
	return string(o)
}

// ToTitle is the human readable string.
func (o CRMObject) ToTitle() string {
	switch o {
	case LineItemsObject:
		return "Line Items"
	case PostalMailObject:
		return "Postal Mail"
	case CustomObjectObject:
		return "Custom Object"
	default:
		return strings.Title(o.String())
	}
}

// CRMObjectFromTitle creates a CRMObject from a title string.
func CRMObjectFromTitle(s string) (CRMObject, error) {
	switch s {
	case "Line Items":
		return LineItemsObject, nil
	case "Postal Mail":
		return PostalMailObject, nil
	case "Custom Object":
		return CustomObjectObject, nil
	default:
		return NewCRMObject(strings.ToLower(s))
	}
}

// String is the underlying string value.
func (o ObjectName) String() string {
	return string(o)
}

// String is the value to use for API calls.
func (o ObjectTypeID) String() string {
	return string(o)
}
