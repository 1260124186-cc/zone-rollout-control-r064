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

	harness.clock.Advance(time.Minute)
	var followup domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0002",
	}, &followup); err != nil {
		return err
	}
	if followup.ID == consistent.ID {
		return ensure(false, "new request key did not produce a follow-up assessment")
	}
	var byRevision domain.RolloutAssessment
	if err := harness.call(
		http.MethodGet,
		"/v1/plans/"+plan.ID+"/assessments/by-revision/"+revision.ID,
		nil,
		&byRevision,
	); err != nil {
		return err
	}
	if byRevision.ID != followup.ID || byRevision.RevisionID != revision.ID || byRevision.PlanID != plan.ID {
		return ensure(false, "revision assessment did not return the last completed assessment")
	}
	var moved domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &moved); err != nil {
		return err
	}
	if moved.ID != followup.ID {
		return ensure(false, "latest assessment did not move to the follow-up assessment")
	}
	err = harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/by-revision/rev_missing", nil, nil)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	err = harness.call(
		http.MethodGet,
		"/v1/plans/"+reviewPlan.ID+"/assessments/by-revision/"+reviewRevision.ID,
		nil,
		nil,
	)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}
	err = harness.call(
		http.MethodGet,
		"/v1/plans/"+plan.ID+"/assessments/by-revision/"+reviewRevision.ID,
		nil,
		nil,
	)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
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
		"/v1/plans/"+reviewPlan.ID+"/assessments/by-revision/"+reviewRevision.ID,
		nil,
		&reviewByRevision,
	); err != nil {
		return err
	}
	if reviewByRevision.ID != review.ID {
		return ensure(false, "revision assessment does not match the review assessment")
	}

	var supersededPlan domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "superseded.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    40,
		ExpectedNodes:       1,
	}, &supersededPlan); err != nil {
		return err
	}
	var oldRevision domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+supersededPlan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5001,
		Authority: "ns1.superseded.example",
		Entries: []domain.ZoneEntry{
			{Owner: "superseded.example", Type: domain.EntryA, Value: "192.0.2.10"},
		},
	}, &oldRevision); err != nil {
		return err
	}
	supersededPlan.Version++
	var newRevision domain.ZoneRevision
	if err := harness.call(http.MethodPatch, "/v1/plans/"+supersededPlan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5002,
		Authority: "ns1.superseded.example",
		Entries: []domain.ZoneEntry{
			{Owner: "superseded.example", Type: domain.EntryA, Value: "192.0.2.11"},
		},
	}, &newRevision); err != nil {
		return err
	}
	supersededPlan.Version++
	if err := harness.call(http.MethodPatch, "/v1/plans/"+supersededPlan.ID+"/activate", map[string]any{
		"expected_version": supersededPlan.Version,
	}, &supersededPlan); err != nil {
		return err
	}
	supersededGroup, err := allocate(harness, supersededPlan.ID, "superseded-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		supersededGroup.LeaseIDs[0],
		"superseded-probe-0001",
		true,
		newRevision.Serial,
		10,
	); err != nil {
		return err
	}
	var superseded domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+supersededPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "superseded-request-0001",
	}, &superseded); err != nil {
		return err
	}
	if superseded.RevisionID != newRevision.ID {
		return ensure(false, "assessment did not target the newest revision")
	}
	var newestByRevision domain.RolloutAssessment
	if err := harness.call(
		http.MethodGet,
		"/v1/plans/"+supersededPlan.ID+"/assessments/by-revision/"+newRevision.ID,
		nil,
		&newestByRevision,
	); err != nil {
		return err
	}
	if newestByRevision.ID != superseded.ID {
		return ensure(false, "newest revision assessment does not match")
	}
	err = harness.call(
		http.MethodGet,
		"/v1/plans/"+supersededPlan.ID+"/assessments/by-revision/"+oldRevision.ID,
		nil,
		nil,
	)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}
	return nil
}
