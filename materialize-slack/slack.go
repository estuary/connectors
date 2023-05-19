package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type SlackSenderConfig struct {
	DisplayName string `json:"display_name" jsonschema:"title=Display Name,description=The display name of the user that messages will originate from,default=Estuary Bot"`
	LogoEmoji   string `json:"logo_emoji,omitempty" jsonschema:"title=Logo Emoji,description=The emoji code to use for the sender's profile picture,default=:ocean:"`
	LogoPicture string `json:"logo_picture,omitempty" jsonschema:"title=Logo Picture,description=The picture to use for the sender's profile"`
}

type SlackAPI struct {
	Token string // Bearer token for authentication

	SenderConfig SlackSenderConfig

	channelIDs map[string]string // Cache for channel Name->ID mappings
}

type slackAuthTestResponse struct {
	Okay  bool   `json:"ok"`
	Error string `json:"error"`
}

// AuthTest invokes the `auth.test` method of the Slack API and returns an error if the test fails.
func (api *SlackAPI) AuthTest() error {
	var resp slackAuthTestResponse
	if err := api.request("GET", "auth.test", nil, &resp); err != nil {
		return err
	}
	if !resp.Okay {
		return fmt.Errorf("slack api error %q", resp.Error)
	}
	return nil
}

type slackPostMessageRequest struct {
	Channel   string          `json:"channel"`          // The channel ID to post to
	Text      string          `json:"text,omitempty"`   // The message to post, as text
	Blocks    json.RawMessage `json:"blocks,omitempty"` // The message to post, as blocks
	Username  string          `json:"username,omitempty"`
	IconEmoji string          `json:"icon_emoji,omitempty"`
	IconUrl   string          `json:"icon_url,omitempty"`
}

type slackPostMessageResponse struct {
	Okay  bool   `json:"ok"`
	Error string `json:"error"` // Error type, if not okay
}

// PostMessage sends a simple message to the specified channel name or ID.
// If the channel is specified by ID it should be prefixed like `id:C1234`.
func (api *SlackAPI) PostMessage(channel, text string, blocks json.RawMessage) error {
	var channelID, err = api.GetChannelID(channel)
	if err != nil {
		return fmt.Errorf("error getting channel id: %w", err)
	}

	var req = slackPostMessageRequest{
		Channel:   channelID,
		Text:      text,
		Blocks:    blocks,
		Username:  api.SenderConfig.DisplayName,
		IconEmoji: api.SenderConfig.LogoEmoji,
		IconUrl:   api.SenderConfig.LogoPicture,
	}

	var resp slackPostMessageResponse
	if err := api.request("POST", "chat.postMessage", &req, &resp); err != nil {
		return err
	}
	if !resp.Okay {
		return fmt.Errorf("slack api error %q", resp.Error)
	}
	return nil
}

type slackListChannelsResponse struct {
	Okay     bool                     `json:"ok"`
	Error    string                   `json:"error"`
	Channels []slackListChannelsEntry `json:"channels"`
}

type slackListChannelsEntry struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (api *SlackAPI) GetChannelID(name string) (string, error) {
	if strings.HasPrefix(name, "id:") {
		return strings.TrimPrefix(name, "id:"), nil
	}

	if api.channelIDs == nil {
		log.Println("fetching channels list...")

		var resp slackListChannelsResponse
		if err := api.request("GET", "conversations.list", nil, &resp); err != nil {
			return "", fmt.Errorf("error listing channels: %w", err)
		}
		if !resp.Okay {
			return "", fmt.Errorf("slack api error %q", resp.Error)
		}

		api.channelIDs = make(map[string]string)
		for _, channel := range resp.Channels {
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

type slackConversationInfoRequest struct {
	Channel string `json:"channel"` // The channel ID to join
}

type slackConversationInfoChannel struct {
	IsMember bool `json:"is_member"`
}
type slackConversationInfoResponse struct {
	Channel slackConversationInfoChannel `json:"channel"`
	Okay    bool                         `json:"ok"`
	Error   string                       `json:"error"`
}

func (api *SlackAPI) ConversationInfo(name string) (*slackConversationInfoResponse, error) {
	var channelId string
	var err error
	if channelId, err = api.GetChannelID(name); err != nil {
		return nil, err
	}

	var resp slackConversationInfoResponse
	if err := api.request("GET", "conversations.info", &slackConversationInfoRequest{Channel: channelId}, &resp); err != nil {
		return nil, fmt.Errorf("error joining channel %s: %w", name, err)
	}
	if !resp.Okay {
		return nil, fmt.Errorf("slack api error when joining channel %s %q", name, resp.Error)
	}

	return &resp, nil
}

type slackJoinChannelRequest struct {
	Channel string `json:"channel"` // The channel ID to join
}

type slackJoinChannelResponse struct {
	Okay  bool   `json:"ok"`
	Error string `json:"error"`
}

func (api *SlackAPI) JoinChannel(name string) error {
	var channelId string
	var err error
	if channelId, err = api.GetChannelID(name); err != nil {
		return err
	}

	var resp slackJoinChannelResponse
	if err := api.request("GET", "conversations.join", &slackJoinChannelRequest{Channel: channelId}, &resp); err != nil {
		return fmt.Errorf("error joining channel %s: %w", name, err)
	}
	if !resp.Okay {
		return fmt.Errorf("slack api error when joining channel %s %q", name, resp.Error)
	}

	return nil
}

func (api *SlackAPI) request(httpMethod, funcName string, req, resp interface{}) error {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("error serializing %q request to JSON: %w", funcName, err)
	}
	reqObject, err := http.NewRequest(httpMethod, "https://slack.com/api/"+funcName, bytes.NewReader(reqBytes))
	if err != nil {
		return fmt.Errorf("error constructing %q request object: %w", funcName, err)
	}
	reqObject.Header.Set("Content-Type", "application/json")
	reqObject.Header.Set("Authorization", "Bearer "+api.Token)
	respObject, err := http.DefaultClient.Do(reqObject)
	if err != nil {
		return fmt.Errorf("error performing %q request: %w", funcName, err)
	}
	defer respObject.Body.Close()
	respBytes, err := ioutil.ReadAll(respObject.Body)
	if err != nil {
		return fmt.Errorf("error reading %q response body: %w", funcName, err)
	}
	if err := json.Unmarshal(respBytes, resp); err != nil {
		return fmt.Errorf("error deserializing %q response from JSON: %w", funcName, err)
	}
	return nil
}
