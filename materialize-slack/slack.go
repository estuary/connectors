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

type slackAPI struct {
	Token string // Bearer token for authentication

	channelIDs map[string]string // Cache for channel Name->ID mappings
}

type slackAuthTestResponse struct {
	Okay  bool   `json:"ok"`
	Error string `json:"error"`
}

// AuthTest invokes the `auth.test` method of the Slack API and returns an error if the test fails.
func (api *slackAPI) AuthTest() error {
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
	Channel string `json:"channel"` // The channel ID to post to
	Text    string `json:"text"`    // The message to post
}

type slackPostMessageResponse struct {
	Okay  bool   `json:"ok"`
	Error string `json:"error"` // Error type, if not okay
}

// PostMessage sends a simple message to the specified channel name or ID.
// If the channel is specified by ID it should be prefixed like `id:C1234`.
func (api *slackAPI) PostMessage(channel, message string) error {
	var channelID, err = api.GetChannelID(channel)
	if err != nil {
		return fmt.Errorf("error getting channel id: %w", err)
	}

	var req = slackPostMessageRequest{Channel: channelID, Text: message}
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

func (api *slackAPI) GetChannelID(name string) (string, error) {
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

func (api *slackAPI) request(httpMethod, funcName string, req, resp interface{}) error {
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

// func main() {
// 	var slack = &slackAPI{
// 		Token: "xoxb-1068976371269-3960963951137-gMUrVV3qQhrMF6qDtMMRFkle",
// 	}
// 	if err := slack.PostMessage("id:U02BEN2RK7E", "Test message from code"); err != nil {
// 		log.Fatalf("test failed: %v", err)
// 	}
// 	log.Println("test passed")
// }
