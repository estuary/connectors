package main

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	slack "github.com/slack-go/slack"
)

type SlackSenderConfig struct {
	DisplayName string `json:"display_name" jsonschema:"title=Display Name,description=The display name of the user that messages will originate from,default=Estuary Bot"`
	LogoEmoji   string `json:"logo_emoji,omitempty" jsonschema:"title=Logo Emoji,description=The emoji code to use for the sender's profile picture,default=:ocean:"`
	LogoPicture string `json:"logo_picture,omitempty" jsonschema:"title=Logo Picture,description=The picture to use for the sender's profile"`
}

type SlackAPI struct {
	Client slack.Client // Bearer token for authentication

	channelIDs map[string]string // Cache for channel Name->ID mappings
}

// AuthTest invokes the `auth.test` method of the Slack API and returns an error if the test fails.
func (api *SlackAPI) AuthTest() error {
	var _, err = api.Client.AuthTest()
	if err != nil {
		return err
	}
	return nil
}

// PostMessage sends a simple message to the specified channel name or ID.
// If the channel is specified by ID it should be prefixed like `id:C1234`.
func (api *SlackAPI) PostMessage(channel, text string, blocks []slack.Block, config SlackSenderConfig) error {
	var channelID, err = api.GetChannelID(channel)
	if err != nil {
		return fmt.Errorf("error getting channel id: %w", err)
	}

	_, _, err = api.Client.PostMessage(
		channelID,
		slack.MsgOptionText(text, false),
		slack.MsgOptionBlocks(blocks...),
		slack.MsgOptionUsername(config.DisplayName),
		slack.MsgOptionIconEmoji(config.LogoEmoji),
		slack.MsgOptionIconURL(config.LogoPicture),
	)
	return err
}

func (api *SlackAPI) GetChannelID(name string) (string, error) {
	if strings.HasPrefix(name, "id:") {
		return strings.TrimPrefix(name, "id:"), nil
	}

	if api.channelIDs == nil {
		var channels []slack.Channel
		var cursor string

		for {
			var found_channels, nextCursor, err = api.Client.GetConversations(&slack.GetConversationsParameters{
				Cursor: cursor,
			})
			if err != nil {
				return "", err
			}
			if nextCursor != "" {
				channels = append(channels, found_channels...)
				cursor = nextCursor
			} else {
				break
			}
		}

		api.channelIDs = make(map[string]string)
		for _, channel := range channels {
			log.Printf("  + channel %q with id %q", channel.Name, channel.ID)
			api.channelIDs[channel.Name] = channel.ID
		}
	}

	var id, ok = api.channelIDs[name]
	if !ok {
		return "", fmt.Errorf("channel %q not found", name)
	}
	return id, nil
}

func (api *SlackAPI) ConversationInfo(name string) (*slack.Channel, error) {
	var channelId string
	var err error
	if channelId, err = api.GetChannelID(name); err != nil {
		return nil, err
	}

	resp, err := api.Client.GetConversationInfo(&slack.GetConversationInfoInput{
		ChannelID: channelId,
	})
	if err != nil {
		return nil, fmt.Errorf("error getting channel info %s: %w", name, err)
	}

	return resp, nil
}

func (api *SlackAPI) JoinChannel(name string) error {
	var channelId string
	var err error
	if channelId, err = api.GetChannelID(name); err != nil {
		return err
	}

	_, _, _, err = api.Client.JoinConversation(channelId)
	if err != nil {
		return fmt.Errorf("error joining channel %s: %w", name, err)
	}

	return nil
}
