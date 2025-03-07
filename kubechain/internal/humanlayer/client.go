package humanlayer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	DefaultAPIURL = "https://api.humanlayer.dev/humanlayer/v1/function_calls"
)

type Client struct {
	apiKey     string
	apiURL     string
	httpClient *http.Client
}

type FunctionCallRequest struct {
	RunID  string `json:"run_id"`
	CallID string `json:"call_id"`
	Spec   Spec   `json:"spec"`
}

type Spec struct {
	Function string                 `json:"fn"`
	Args     map[string]interface{} `json:"kwargs"`
}

type FunctionCallResponse struct {
	CallID string `json:"call_id"`
	Status Status `json:"status,omitempty"`
}

type Status struct {
	Approved bool   `json:"approved"`
	Comment  string `json:"comment,omitempty"`
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		apiURL: DefaultAPIURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) CallFunction(ctx context.Context, runID, callID string, function string, args map[string]interface{}) (*FunctionCallResponse, error) {
	request := FunctionCallRequest{
		RunID:  runID,
		CallID: callID,
		Spec: Spec{
			Function: function,
			Args:     args,
		},
	}

	requestJSON, _ := json.MarshalIndent(request, "", "  ")
	fmt.Printf("Request payload: %s\n", string(requestJSON))

	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.apiURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned non-success status: %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var response FunctionCallResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}

func (c *Client) PollForApproval(ctx context.Context, callID string) (*FunctionCallResponse, error) {
	statusURL := fmt.Sprintf("%s/%s", c.apiURL, callID)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			req, err := http.NewRequestWithContext(ctx, "GET", statusURL, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create status request: %w", err)
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

			resp, err := c.httpClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("status request failed: %w", err)
			}

			var statusResponse FunctionCallResponse
			err = json.NewDecoder(resp.Body).Decode(&statusResponse)
			resp.Body.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to parse status response: %w", err)
			}

			if statusResponse.Status.Approved {
				fmt.Printf("Approval granted with comment: %s\n", statusResponse.Status.Comment)
				return &statusResponse, nil
			}

			fmt.Println("Waiting for approval...")
			time.Sleep(2 * time.Second)
		}
	}
}
