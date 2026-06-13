package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	arg "github.com/alexflint/go-arg"
	hubspot "github.com/estuary/connectors/materialize-hubspot"
	"github.com/sirupsen/logrus"
)

func parseConfig(filename string) (*hubspot.Config, error) {
	var f *os.File
	if filename == "-" {
		f = os.Stdin
	} else {
		var err error
		f, err = os.Open(filename)
		if err != nil {
			return nil, err
		}
		defer f.Close()
	}
	input, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	var config hubspot.Config
	err = json.Unmarshal(input, &config)
	if err != nil {
		return nil, err
	}

	err = config.Validate()
	if err != nil {
		return nil, err
	}

	return &config, nil
}

type RefreshTokensCmd struct{}

func (*RefreshTokensCmd) Run(ctx context.Context, client *hubspot.Client) error {
	now := time.Now()
	tokens, err := client.RefreshTokens(ctx, now)
	if err != nil {
		return err
	}

	update := map[string]string{
		"refresh_token":           tokens.RefreshToken.Expose(),
		"access_token":            tokens.AccessToken.Expose(),
		"access_token_expires_at": tokens.AccessTokenExpiresAt.Format(time.RFC3339),
	}

	data, err := json.MarshalIndent(update, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

type InspectTokensCmd struct{}

func (*InspectTokensCmd) Run(ctx context.Context, client *hubspot.Client) error {
	tokens, err := client.InspectTokens(ctx)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(tokens, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

type CreatePropertyCmd struct {
	Object      string                    `arg:"positional,required"`
	Name        string                    `arg:"--name,required"`
	GroupName   string                    `arg:"--group,required"`
	Label       string                    `arg:"--label"`
	Type        hubspot.PropertyType      `arg:"--type" default:"string"`
	FieldType   hubspot.PropertyFieldType `arg:"--field-type" default:"text"`
	Description string                    `arg:"--desc"`
	Unique      bool                      `arg:"--unique"`
}

func (c *CreatePropertyCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	property := &hubspot.Property{
		Name:           c.Name,
		Type:           c.Type,
		FieldType:      c.FieldType,
		Description:    c.Description,
		GroupName:      c.GroupName,
		HasUniqueValue: c.Unique,
	}

	return client.CreateProperty(ctx, object, property)
}

type GetPropertyCmd struct {
	Object string `arg:"positional,required"`
	Name   string `arg:"--name,required"`
}

func (c *GetPropertyCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	property, err := client.GetProperty(ctx, object, c.Name)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(property, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

type ListPropertiesCmd struct {
	Object string `arg:"positional,required"`
}

func (c *ListPropertiesCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	properties, err := client.ListProperties(ctx, object)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(properties, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

type DeletePropertyCmd struct {
	Object string `arg:"positional,required"`
	Name   string `arg:"required"`
}

func (c *DeletePropertyCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	return client.DeleteProperty(ctx, object, c.Name)
}

type CreatePropertyGroupCmd struct {
	Object string `arg:"positional,required"`
	Name   string `arg:"--name,required"`
	Label  string `arg:"--label"`
}

func (c *CreatePropertyGroupCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	group := &hubspot.PropertyGroup{
		Archived:     false,
		Name:         c.Name,
		Label:        c.Label,
		DisplayOrder: hubspot.DisplayOrderLast,
	}

	err = client.CreatePropertyGroup(ctx, object, group)
	if err != nil {
		return err
	}

	return nil
}

type GetPropertyGroupCmd struct {
	Object string `arg:"positional,required"`
	Name   string `arg:"--name,required"`
}

func (c *GetPropertyGroupCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	group, err := client.GetPropertyGroup(ctx, object, c.Name)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(group, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

type DeletePropertyGroupCmd struct {
	Object string `arg:"positional,required"`
	Name   string `arg:"--name,required"`
}

func (c *DeletePropertyGroupCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	return client.DeletePropertyGroup(ctx, object, c.Name)
}

type ListPropertyGroupsCmd struct {
	Object string `arg:"positional,required"`
}

func (c *ListPropertyGroupsCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	groups, err := client.ListPropertyGroups(ctx, object)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(groups, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

type BatchReadCmd struct {
	Object     string   `arg:"positional,required"`
	IDProperty string   `arg:"--idproperty,required"`
	Inputs     []string `arg:"--inputs,separate,required"`
	Properties []string `arg:"--properties,separate"`
}

func (c *BatchReadCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	read := &hubspot.BatchReadRequest{
		IDProperty: c.IDProperty,
		Inputs:     hubspot.NewBatchReadInputs(c.Inputs),
		Properties: c.Properties,
	}

	batch, err := client.BatchRead(ctx, object, read)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(batch, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

type SearchEqualCmd struct {
	Object     string   `arg:"positional,required"`
	IDProperty string   `arg:"--idproperty,required"`
	Values     []string `arg:"--values,separate,required"`
	Properties []string `arg:"--properties,separate"`
}

func (c *SearchEqualCmd) Run(ctx context.Context, client *hubspot.Client) error {
	object, err := hubspot.NewCRMObject(c.Object)
	if err != nil {
		return err
	}

	request := &hubspot.SearchRequest{
		FilterGroups: hubspot.NewFilterGroupsEquals(c.IDProperty, c.Values),
	}
	if len(c.Properties) > 0 {
		request.Properties = c.Properties
	}

	results, err := client.Search(ctx, object, request)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(results, "", "    ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

type Args struct {
	Config string `arg:"-c,--config" default:"-"`

	RefreshTokens       *RefreshTokensCmd       `arg:"subcommand:refresh-tokens"`
	InspectTokens       *InspectTokensCmd       `arg:"subcommand:inspect-tokens"`
	CreateProperty      *CreatePropertyCmd      `arg:"subcommand:create-property"`
	DeleteProperty      *DeletePropertyCmd      `arg:"subcommand:delete-property"`
	GetProperty         *GetPropertyCmd         `arg:"subcommand:get-property"`
	ListProperties      *ListPropertiesCmd      `arg:"subcommand:list-properties"`
	CreatePropertyGroup *CreatePropertyGroupCmd `arg:"subcommand:create-property-group"`
	DeletePropertyGroup *DeletePropertyGroupCmd `arg:"subcommand:delete-property-group"`
	GetPropertyGroup    *GetPropertyGroupCmd    `arg:"subcommand:get-property-group"`
	ListPropertyGroups  *ListPropertyGroupsCmd  `arg:"subcommand:list-property-groups"`

	BatchRead   *BatchReadCmd   `arg:"subcommand:batch-read"`
	SearchEqual *SearchEqualCmd `arg:"subcommand:search-equal"`
}

func main() {
	args := Args{}
	argParser, err := arg.NewParser(arg.Config{}, &args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to create parser: %v\n", err)
	}

	err = argParser.Parse(os.Args[1:])
	switch {
	case err == arg.ErrHelp:
		argParser.WriteHelp(os.Stdout)
		os.Exit(0)
	case err != nil:
		fmt.Printf("unable to parse arguments: %v\n", err)
		argParser.WriteUsage(os.Stderr)
		os.Exit(1)
	}
	if argParser.Subcommand() == nil {
		argParser.Fail("missing subcommand")
	}

	config, err := parseConfig(args.Config)
	if err != nil {
		logrus.WithError(err).Error("unable to parse config")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	client, err := hubspot.NewClient(config.Credentials, config.Limiter(), config.SearchLimiter())
	if err != nil {
		logrus.WithError(err).Error("unable to create client")
		os.Exit(1)
	}

	switch {
	case args.RefreshTokens != nil:
		err = args.RefreshTokens.Run(ctx, client)
	case args.InspectTokens != nil:
		err = args.InspectTokens.Run(ctx, client)
	case args.CreateProperty != nil:
		err = args.CreateProperty.Run(ctx, client)
	case args.GetProperty != nil:
		err = args.GetProperty.Run(ctx, client)
	case args.ListProperties != nil:
		err = args.ListProperties.Run(ctx, client)
	case args.DeleteProperty != nil:
		err = args.DeleteProperty.Run(ctx, client)
	case args.ListPropertyGroups != nil:
		err = args.ListPropertyGroups.Run(ctx, client)
	case args.CreatePropertyGroup != nil:
		err = args.CreatePropertyGroup.Run(ctx, client)
	case args.GetPropertyGroup != nil:
		err = args.GetPropertyGroup.Run(ctx, client)
	case args.DeletePropertyGroup != nil:
		err = args.DeletePropertyGroup.Run(ctx, client)
	case args.BatchRead != nil:
		err = args.BatchRead.Run(ctx, client)
	case args.SearchEqual != nil:
		err = args.SearchEqual.Run(ctx, client)
	default:
		err = fmt.Errorf("unimplemented command")
	}

	if err != nil {
		logrus.WithError(err).Error("exiting with error")
		os.Exit(1)
	}
}
