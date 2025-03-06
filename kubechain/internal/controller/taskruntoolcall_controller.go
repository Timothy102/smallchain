package controller

import (
	"bytes"
	"net/http"
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kubechainv1alpha1 "github.com/humanlayer/smallchain/kubechain/api/v1alpha1"
)

// TaskRunToolCallReconciler reconciles a TaskRunToolCall object.
type TaskRunToolCallReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	recorder record.EventRecorder
}

// Reconcile processes TaskRunToolCall objects.
func (r *TaskRunToolCallReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var trtc kubechainv1alpha1.TaskRunToolCall
	if err := r.Get(ctx, req.NamespacedName, &trtc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	logger.Info("Reconciling TaskRunToolCall", "name", trtc.Name)

	// Initialize status if not set.
	if trtc.Status.Phase == "" {
		trtc.Status.Phase = kubechainv1alpha1.TaskRunToolCallPhasePending
		trtc.Status.Status = "Pending"
		trtc.Status.StatusDetail = "Initializing"
		trtc.Status.StartTime = &metav1.Time{Time: time.Now()}
		if err := r.Status().Update(ctx, &trtc); err != nil {
			logger.Error(err, "Failed to update initial status on TaskRunToolCall")
			return ctrl.Result{}, err
		}
		// Requeue so we pick up the updated status.
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if a child TaskRun already exists for this tool call.
	var taskRunList kubechainv1alpha1.TaskRunList
	if err := r.List(ctx, &taskRunList, client.InNamespace(trtc.Namespace), client.MatchingLabels{"kubechain.humanlayer.dev/taskruntoolcall": trtc.Name}); err != nil {
		logger.Error(err, "Failed to list child TaskRuns")
		return ctrl.Result{}, err
	}
	if len(taskRunList.Items) > 0 {
		logger.Info("Child TaskRun already exists", "childTaskRun", taskRunList.Items[0].Name)
		// Optionally, sync status from child to parent.
		return ctrl.Result{}, nil
	}

	// Fetch the referenced Tool.
	var tool kubechainv1alpha1.Tool
	if err := r.Get(ctx, client.ObjectKey{Namespace: trtc.Namespace, Name: trtc.Spec.ToolRef.Name}, &tool); err != nil {
		logger.Error(err, "Failed to get Tool", "tool", trtc.Spec.ToolRef.Name)
		trtc.Status.Status = "Error"
		trtc.Status.StatusDetail = fmt.Sprintf("Failed to get Tool: %v", err)
		trtc.Status.Error = err.Error()
		r.recorder.Event(&trtc, corev1.EventTypeWarning, "ValidationFailed", err.Error())
		if err := r.Status().Update(ctx, &trtc); err != nil {
			logger.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, err
	}

	// Handle different tool types
	if tool.Spec.ToolType == "delegateToAgent" {
		err := fmt.Errorf("delegation is not implemented yet; only direct execution is supported")
		logger.Error(err, "Delegation not implemented")
		trtc.Status.Status = "Error"
		trtc.Status.StatusDetail = err.Error()
		trtc.Status.Error = err.Error()
		r.recorder.Event(&trtc, corev1.EventTypeWarning, "ValidationFailed", err.Error())
		if err := r.Status().Update(ctx, &trtc); err != nil {
			logger.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, err
	} else if tool.Spec.ToolType == "function" {
		// Execute built-in function directly.
		var args map[string]float64
		if err := json.Unmarshal([]byte(trtc.Spec.Arguments), &args); err != nil {
			logger.Error(err, "Failed to parse arguments")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = "Invalid arguments JSON"
			trtc.Status.Error = err.Error()
			r.recorder.Event(&trtc, corev1.EventTypeWarning, "ExecutionFailed", err.Error())
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}

		var res float64
		switch tool.Spec.Execute.Builtin.Name {
		case "add":
			res = args["a"] + args["b"]
		case "subtract":
			res = args["a"] - args["b"]
		case "multiply":
			res = args["a"] * args["b"]
		case "divide":
			if args["b"] == 0 {
				err := fmt.Errorf("division by zero")
				logger.Error(err, "Division by zero")
				trtc.Status.Status = "Error"
				trtc.Status.StatusDetail = "Division by zero"
				trtc.Status.Error = err.Error()
				r.recorder.Event(&trtc, corev1.EventTypeWarning, "ExecutionFailed", err.Error())
				if err := r.Status().Update(ctx, &trtc); err != nil {
					logger.Error(err, "Failed to update status")
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, err
			}
			res = args["a"] / args["b"]
		default:
			err := fmt.Errorf("unsupported builtin function %q", tool.Spec.Execute.Builtin.Name)
			logger.Error(err, "Unsupported builtin")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = err.Error()
			trtc.Status.Error = err.Error()
			r.recorder.Event(&trtc, corev1.EventTypeWarning, "ExecutionFailed", err.Error())
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}

		// Update TaskRunToolCall status with the function result.
		trtc.Status.Result = fmt.Sprintf("%v", res)
		trtc.Status.Phase = kubechainv1alpha1.TaskRunToolCallPhaseSucceeded
		trtc.Status.Status = "Ready"
		trtc.Status.StatusDetail = "Tool executed successfully"
		if err := r.Status().Update(ctx, &trtc); err != nil {
			logger.Error(err, "Failed to update TaskRunToolCall status after execution")
			return ctrl.Result{}, err
		}
		logger.Info("Direct execution completed", "result", res)
		r.recorder.Event(&trtc, corev1.EventTypeNormal, "ExecutionSucceeded", fmt.Sprintf("Tool %q executed successfully", tool.Name))
		return ctrl.Result{}, nil
	} else if tool.Spec.ToolType == "externalAPI" {
		// Handle external API tool call
		if tool.Spec.Execute.ExternalAPI == nil {
			err := fmt.Errorf("externalAPI tool missing execution details")
			logger.Error(err, "Missing execution details")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = err.Error()
			trtc.Status.Error = err.Error()
			r.recorder.Event(&trtc, corev1.EventTypeWarning, "ValidationFailed", err.Error())
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Get API key from secret
		var apiKey string
		if tool.Spec.Execute.ExternalAPI.CredentialsFrom != nil {
			var secret corev1.Secret
			err := r.Get(ctx, client.ObjectKey{
				Namespace: trtc.Namespace,
				Name:      tool.Spec.Execute.ExternalAPI.CredentialsFrom.Name,
			}, &secret)
			if err != nil {
				logger.Error(err, "Failed to get API credentials")
				trtc.Status.Status = "Error"
				trtc.Status.StatusDetail = fmt.Sprintf("Failed to get API credentials: %v", err)
				trtc.Status.Error = err.Error()
				r.recorder.Event(&trtc, corev1.EventTypeWarning, "ValidationFailed", err.Error())
				if err := r.Status().Update(ctx, &trtc); err != nil {
					logger.Error(err, "Failed to update status")
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, err
			}
			
			apiKey = string(secret.Data[tool.Spec.Execute.ExternalAPI.CredentialsFrom.Key])
			if apiKey == "" {
				err := fmt.Errorf("empty API key in secret")
				logger.Error(err, "Empty API key")
				trtc.Status.Status = "Error"
				trtc.Status.StatusDetail = err.Error()
				trtc.Status.Error = err.Error()
				r.recorder.Event(&trtc, corev1.EventTypeWarning, "ValidationFailed", err.Error())
				if err := r.Status().Update(ctx, &trtc); err != nil {
					logger.Error(err, "Failed to update status")
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, err
			}
		}
		
		// Parse the arguments for the API call
		var args interface{}
		if err := json.Unmarshal([]byte(trtc.Spec.Arguments), &args); err != nil {
			logger.Error(err, "Failed to parse arguments")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = "Invalid arguments JSON"
			trtc.Status.Error = err.Error()
			r.recorder.Event(&trtc, corev1.EventTypeWarning, "ExecutionFailed", err.Error())
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Make the API request
		logger.Info("Making external API call", "url", tool.Spec.Execute.ExternalAPI.URL)
		
		// Prepare the request
		reqBody, err := json.Marshal(args)
		if err != nil {
			logger.Error(err, "Failed to marshal request body")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = fmt.Sprintf("Failed to prepare request: %v", err)
			trtc.Status.Error = err.Error()
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Create HTTP client with timeout
		client := &http.Client{
			Timeout: 30 * time.Second,
		}
		
		// Create request
		req, err := http.NewRequestWithContext(
			ctx, 
			"POST", 
			tool.Spec.Execute.ExternalAPI.URL, 
			bytes.NewBuffer(reqBody),
		)
		if err != nil {
			logger.Error(err, "Failed to create HTTP request")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = fmt.Sprintf("Failed to create HTTP request: %v", err)
			trtc.Status.Error = err.Error()
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Set headers
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))
		}
		
		// Execute the request
		resp, err := client.Do(req)
		if err != nil {
			logger.Error(err, "API request failed")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = fmt.Sprintf("API request failed: %v", err)
			trtc.Status.Error = err.Error()
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		defer resp.Body.Close()
		
		// Check for non-success status code
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err := fmt.Errorf("API returned non-success status: %d", resp.StatusCode)
			logger.Error(err, "API returned error status")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = err.Error()
			trtc.Status.Error = err.Error()
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Read and parse the response
		var result interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			logger.Error(err, "Failed to parse API response")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = fmt.Sprintf("Failed to parse API response: %v", err)
			trtc.Status.Error = err.Error()
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Convert result to JSON string
		resultJSON, err := json.Marshal(result)
		if err != nil {
			logger.Error(err, "Failed to marshal result")
			trtc.Status.Status = "Error"
			trtc.Status.StatusDetail = fmt.Sprintf("Failed to format result: %v", err)
			trtc.Status.Error = err.Error()
			if err := r.Status().Update(ctx, &trtc); err != nil {
				logger.Error(err, "Failed to update status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, err
		}
		
		// Update TaskRunToolCall with the result
		trtc.Status.Result = string(resultJSON)
		trtc.Status.Phase = kubechainv1alpha1.TaskRunToolCallPhaseSucceeded
		trtc.Status.Status = "Ready"
		trtc.Status.StatusDetail = "API call executed successfully"
		if err := r.Status().Update(ctx, &trtc); err != nil {
			logger.Error(err, "Failed to update TaskRunToolCall status after execution")
			return ctrl.Result{}, err
		}
		
		logger.Info("API execution completed", "statusCode", resp.StatusCode)
		r.recorder.Event(&trtc, corev1.EventTypeNormal, "ExecutionSucceeded", 
			fmt.Sprintf("API call to %s executed successfully", tool.Spec.Execute.ExternalAPI.URL))
		return ctrl.Result{}, nil
	}

	// Fallback: if tool type is not recognized.
	err := fmt.Errorf("unsupported tool type %q", tool.Spec.ToolType)
	logger.Error(err, "Unsupported tool type")
	trtc.Status.Status = "Error"
	trtc.Status.StatusDetail = err.Error()
	trtc.Status.Error = err.Error()
	r.recorder.Event(&trtc, corev1.EventTypeWarning, "ExecutionFailed", err.Error())
	if err := r.Status().Update(ctx, &trtc); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// SetupWithManager sets up this controller with the Manager.
func (r *TaskRunToolCallReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.recorder = mgr.GetEventRecorderFor("taskruntoolcall-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&kubechainv1alpha1.TaskRunToolCall{}).
		Complete(r)
}
