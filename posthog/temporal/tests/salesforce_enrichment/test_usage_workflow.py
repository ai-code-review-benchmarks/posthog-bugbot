import json

import pytest

from django.test import TestCase

from posthog.temporal.salesforce_enrichment.usage_workflow import (
    SalesforceOrgMapping,
    SalesforceUsageEnrichmentWorkflow,
    SalesforceUsageUpdate,
    UsageEnrichmentInputs,
    prepare_salesforce_update_record,
)

from ee.billing.salesforce_enrichment.usage_signals import UsageSignals


class TestSalesforceOrgMapping(TestCase):
    def test_create_mapping(self):
        mapping = SalesforceOrgMapping(
            salesforce_account_id="001ABC123",
            posthog_org_id="550e8400-e29b-41d4-a716-446655440000",
        )

        assert mapping.salesforce_account_id == "001ABC123"
        assert mapping.posthog_org_id == "550e8400-e29b-41d4-a716-446655440000"


class TestSalesforceUsageUpdate(TestCase):
    def test_create_update(self):
        signals = UsageSignals(
            active_users_7d=100,
            active_users_30d=500,
            sessions_7d=200,
        )
        update = SalesforceUsageUpdate(
            salesforce_account_id="001ABC123",
            signals=signals,
        )

        assert update.salesforce_account_id == "001ABC123"
        assert update.signals.active_users_7d == 100


class TestUsageEnrichmentInputs(TestCase):
    def test_default_values(self):
        inputs = UsageEnrichmentInputs()

        assert inputs.batch_size == 100
        assert inputs.max_orgs is None
        assert inputs.specific_org_id is None

    def test_with_values(self):
        inputs = UsageEnrichmentInputs(
            batch_size=50,
            max_orgs=1000,
            specific_org_id="org-123",
        )

        assert inputs.batch_size == 50
        assert inputs.max_orgs == 1000
        assert inputs.specific_org_id == "org-123"


class TestPrepareSalesforceUpdateRecord(TestCase):
    def test_basic_signals(self):
        signals = UsageSignals(
            active_users_7d=100,
            active_users_30d=500,
            sessions_7d=200,
            sessions_30d=800,
            events_per_session_7d=10.5,
            events_per_session_30d=9.8,
            products_activated_7d=["analytics", "recordings"],
            products_activated_30d=["analytics", "recordings", "feature_flags"],
            days_since_last_login=3,
        )

        record = prepare_salesforce_update_record("001ABC123", signals)

        assert record["Id"] == "001ABC123"
        assert record["posthog_active_users_7d__c"] == 100
        assert record["posthog_active_users_30d__c"] == 500
        assert record["posthog_sessions_7d__c"] == 200
        assert record["posthog_sessions_30d__c"] == 800
        assert record["posthog_events_per_session_7d__c"] == 10.5
        assert record["posthog_events_per_session_30d__c"] == 9.8
        assert record["posthog_products_7d__c"] == "analytics,recordings"
        assert record["posthog_products_30d__c"] == "analytics,feature_flags,recordings"
        assert record["posthog_last_login_days__c"] == 3

    def test_with_momentum(self):
        signals = UsageSignals(
            active_users_7d=100,
            active_users_30d=500,
            active_users_7d_momentum=25.5,
            active_users_30d_momentum=-10.2,
            sessions_7d_momentum=15.0,
            sessions_30d_momentum=5.0,
            events_per_session_7d_momentum=2.5,
            events_per_session_30d_momentum=-1.5,
        )

        record = prepare_salesforce_update_record("001ABC123", signals)

        assert record["posthog_active_users_7d_momentum__c"] == 25.5
        assert record["posthog_active_users_30d_momentum__c"] == -10.2
        assert record["posthog_sessions_7d_momentum__c"] == 15.0
        assert record["posthog_sessions_30d_momentum__c"] == 5.0
        assert record["posthog_eps_7d_momentum__c"] == 2.5
        assert record["posthog_eps_30d_momentum__c"] == -1.5

    def test_none_values_excluded(self):
        signals = UsageSignals(
            active_users_7d=100,
            events_per_session_7d=None,
            days_since_last_login=None,
            active_users_7d_momentum=None,
        )

        record = prepare_salesforce_update_record("001ABC123", signals)

        assert record["Id"] == "001ABC123"
        assert record["posthog_active_users_7d__c"] == 100
        assert "posthog_events_per_session_7d__c" not in record
        assert "posthog_last_login_days__c" not in record
        assert "posthog_active_users_7d_momentum__c" not in record

    def test_per_user_metrics(self):
        signals = UsageSignals(
            insights_per_user_7d=2.5,
            insights_per_user_30d=3.2,
            dashboards_per_user_7d=1.0,
            dashboards_per_user_30d=1.5,
        )

        record = prepare_salesforce_update_record("001ABC123", signals)

        assert record["posthog_insights_per_user_7d__c"] == 2.5
        assert record["posthog_insights_per_user_30d__c"] == 3.2
        assert record["posthog_dashboards_per_user_7d__c"] == 1.0
        assert record["posthog_dashboards_per_user_30d__c"] == 1.5

    def test_empty_products_list(self):
        signals = UsageSignals(
            products_activated_7d=[],
            products_activated_30d=[],
        )

        record = prepare_salesforce_update_record("001ABC123", signals)

        assert record["posthog_products_7d__c"] == ""
        assert record["posthog_products_30d__c"] == ""


class TestWorkflowParseInputs(TestCase):
    def test_parse_inputs_valid_json(self):
        inputs = SalesforceUsageEnrichmentWorkflow.parse_inputs(['{"batch_size": 50}'])

        assert inputs.batch_size == 50
        assert inputs.max_orgs is None
        assert inputs.specific_org_id is None

    def test_parse_inputs_all_fields(self):
        inputs = SalesforceUsageEnrichmentWorkflow.parse_inputs(
            ['{"batch_size": 25, "max_orgs": 100, "specific_org_id": "org-123"}']
        )

        assert inputs.batch_size == 25
        assert inputs.max_orgs == 100
        assert inputs.specific_org_id == "org-123"

    def test_parse_inputs_empty_json(self):
        inputs = SalesforceUsageEnrichmentWorkflow.parse_inputs(["{}"])

        assert inputs.batch_size == 100
        assert inputs.max_orgs is None
        assert inputs.specific_org_id is None

    def test_parse_inputs_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            SalesforceUsageEnrichmentWorkflow.parse_inputs(["invalid"])
