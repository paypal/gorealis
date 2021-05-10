package realis

import (
	"context"

	"github.com/paypal/gorealis/gen-go/apache/aurora"
)

func (r *realisClient) jobExists(key aurora.JobKey) bool {
	resp, err := r.client.GetConfigSummary(context.TODO(), &key)
	if err != nil {
		return false
	}

	return resp == nil ||
		resp.GetResult_() == nil ||
		resp.GetResult_().GetConfigSummaryResult_() == nil ||
		resp.GetResult_().GetConfigSummaryResult_().GetSummary() == nil ||
		resp.GetResponseCode() != aurora.ResponseCode_OK
}
