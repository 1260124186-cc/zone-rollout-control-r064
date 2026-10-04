package workflowprobe

import (
	"fmt"
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runConsistencyAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-assess-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-assess-02", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "assess.example", 3001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "assess-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("assess-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	var consistent domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &consistent); err != nil {
		return err
	}
	if consistent.Verdict != domain.VerdictConsistent || consistent.ProbeCount != 2 {
		return ensure(false, "consistent assessment did not include both probes")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != consistent.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != consistent.ID {
		return ensure(false, "latest assessment does not match")
	}

	var byRevision domain.RolloutAssessment
	if err := harness.call(
		http.MethodGet,
		"/v1/plans/"+plan.ID+"/revisions/"+revision.ID+"/assessment",
		nil,
		&byRevision,
	); err != nil {
		return err
	}
	if byRevision.ID != consistent.ID || byRevision.RevisionID != revision.ID || byRevision.PlanID != plan.ID {
		return ensure(false, "revision assessment does not match the consistent assessment")
	}

	harness.clock.Advance(time.Minute)
	var second domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0002",
	}, &second); err != nil {
		return err
	}
	if second.ID == consistent.ID {
		return ensure(false, "second assessment was not stored separately")
	}
	var latestForRevision domain.RolloutAssessment
	if err := harness.call(
		http.MethodGet,
		"/v1/plans/"+plan.ID+"/revisions/"+revision.ID+"/assessment",
		nil,
		&latestForRevision,
	); err != nil {
		return err
	}
	if latestForRevision.ID != second.ID {
		return ensure(false, "revision assessment did not return the last completed assessment")
	}
	var firstReplay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &firstReplay); err != nil {
		return err
	}
	if firstReplay.ID != consistent.ID {
		return ensure(false, "same-key replay did not return the original assessment")
	}
	var newest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &newest); err != nil {
		return err
	}
	if newest.ID != second.ID {
		return ensure(false, "latest assessment did not move to the second assessment")
	}
	err = harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/revisions/rev_missing/assessment", nil, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	); err != nil {
		return err
	}
	err = harness.call(
		http.MethodGet,
		"/v1/plans/"+reviewPlan.ID+"/revisions/"+reviewRevision.ID+"/assessment",
		nil,
		nil,
	)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &review); err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "mismatched serial did not require review")
	}
	var reviewByRevision domain.RolloutAssessment
	if err := harness.call(
		http.MethodGet,
		"/v1/plans/"+reviewPlan.ID+"/revisions/"+reviewRevision.ID+"/assessment",
		nil,
		&reviewByRevision,
	); err != nil {
		return err
	}
	if reviewByRevision.ID != review.ID {
		return ensure(false, "review assessment was not located by revision")
	}
	err = harness.call(
		http.MethodGet,
		"/v1/plans/"+reviewPlan.ID+"/revisions/"+revision.ID+"/assessment",
		nil,
		nil,
	)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	return runRevisionHistoryAssessment(harness)
}

// runRevisionHistoryAssessment covers a plan whose draft history holds a
// superseded revision that never reached activation. The superseded revision
// must answer with a clear empty state instead of the assessment that was
// completed for the plan's latest revision.
func runRevisionHistoryAssessment(harness *harness) error {
	var historyPlan domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "history.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    60,
		ExpectedNodes:       1,
	}, &historyPlan); err != nil {
		return err
	}
	var superseded domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+historyPlan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5001,
		Authority: "ns1.history.example",
		Entries: []domain.ZoneEntry{
			{Owner: "history.example", Type: domain.EntryA, Value: "192.0.2.30"},
		},
	}, &superseded); err != nil {
		return err
	}
	var current domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+historyPlan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5002,
		Authority: "ns1.history.example",
		Entries: []domain.ZoneEntry{
			{Owner: "history.example", Type: domain.EntryA, Value: "192.0.2.31"},
		},
	}, &current); err != nil {
		return err
	}
	historyPlan.Version += 2
	if err := harness.call(http.MethodPatch, "/v1/plans/"+historyPlan.ID+"/activate", map[string]any{
		"expected_version": historyPlan.Version,
	}, &historyPlan); err != nil {
		return err
	}
	group, err := allocate(harness, historyPlan.ID, "history-lease-01", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(harness, group.LeaseIDs[0], "history-probe-01", true, current.Serial, 10); err != nil {
		return err
	}
	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+historyPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "history-request-1",
	}, &assessment); err != nil {
		return err
	}
	if assessment.RevisionID != current.ID {
		return ensure(false, "assessment did not reference the latest revision")
	}
	var currentAssessment domain.RolloutAssessment
	if err := harness.call(
		http.MethodGet,
		"/v1/plans/"+historyPlan.ID+"/revisions/"+current.ID+"/assessment",
		nil,
		&currentAssessment,
	); err != nil {
		return err
	}
	if currentAssessment.ID != assessment.ID {
		return ensure(false, "latest revision assessment was not located")
	}
	err = harness.call(
		http.MethodGet,
		"/v1/plans/"+historyPlan.ID+"/revisions/"+superseded.ID+"/assessment",
		nil,
		nil,
	)
	return expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound))
}
