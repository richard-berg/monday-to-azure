import json
import logging

import azure.durable_functions as durable_func
import azure.functions as func
from azure.durable_functions import DurableOrchestrationClient
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient

import gaggle_fns
import msgraph_fns
from monday_fns import MondayUser, get_monday_roster

AZURE_VAULT_URL = "https://cantorivault.vault.azure.net/"
MONDAY_SECRET_NAME = "monday-api-key"
GAGGLE_SECRET_NAME = "gaggle-api-key"

app = durable_func.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="group/{group:alpha}")
async def get_group_members(req: func.HttpRequest) -> func.HttpResponse:
    group = req.route_params.get("group")
    try:
        names = await msgraph_fns.get_group_member_names(group)
        return func.HttpResponse("\n".join(sorted(names)))
    except ValueError as e:
        return func.HttpResponse(str(e), status_code=404)


@app.route(route="groups")
async def get_groups(req: func.HttpRequest) -> func.HttpResponse:
    groups = await msgraph_fns.get_group_email_prefixes()
    return func.HttpResponse("\n".join(sorted(groups)))


@app.schedule(schedule="42 42 */6 * * *", arg_name="mytimer")
@app.durable_client_input(client_name="client")
async def scheduled_sync(mytimer: func.TimerRequest, client: DurableOrchestrationClient) -> None:
    instance_id = await client.start_new(monday_sync_orchestrator._function._name, client_input={})
    logging.info(f"Started periodic Monday->AAD sync with instance_id {instance_id}")


@app.route(route="monday-webhook")
@app.durable_client_input(client_name="client")
async def monday_webhook(req: func.HttpRequest, client: DurableOrchestrationClient) -> func.HttpResponse:
    challenge, event = _parse_webhook_body(req)
    instance_id = await client.start_new(monday_sync_orchestrator._function._name, client_input=event)
    response = client.create_check_status_response(req, instance_id)
    return _insert_challenge(challenge, response)


def _parse_webhook_body(request: func.HttpRequest) -> tuple[str, dict]:
    try:
        request_body = request.get_body()
        request_json = json.loads(request_body[:1000])  # prevent DOS
        challenge = request_json.get("challenge", "")
        event = request_json.get("event", {})
        return challenge, event
    except Exception:
        return "", {}


def _insert_challenge(challenge: str, response: func.HttpResponse) -> func.HttpResponse:
    """Enrich the HttpResponse generated by Durable Function so that it includes Monday.com's challenge string
    See: https://support.monday.com/hc/en-us/articles/360003540679-Webhook-Integration-?_ga=2.108678859.838519620.1692582277-1931607118.1665512780
    """
    response_body = response.get_body()
    response_json = json.loads(response_body)
    response_json["challenge"] = challenge
    return func.HttpResponse(
        body=json.dumps(response_json),
        charset=response.charset,
        headers=response.headers,
        mimetype=response.mimetype,
        status_code=response.status_code,
    )


@app.orchestration_trigger(context_name="context")
def monday_sync_orchestrator(context: durable_func.DurableOrchestrationContext):
    retry_options = durable_func.RetryOptions(5000, 2)
    webhook_event = context.get_input()
    roster = yield context.call_activity_with_retry(pull_from_monday, retry_options, webhook_event)
    gaggle_result = yield context.call_activity_with_retry(push_to_gaggle, retry_options, roster)
    aad_result = yield context.call_activity_with_retry(push_to_aad, retry_options, roster)

    return aad_result + gaggle_result


@app.activity_trigger(input_name="webhookEvent")
async def pull_from_monday(webhookEvent: dict) -> list[MondayUser]:
    async with DefaultAzureCredential() as credential:
        async with SecretClient(vault_url=AZURE_VAULT_URL, credential=credential) as secret_client:
            monday_api_key = await secret_client.get_secret(MONDAY_SECRET_NAME)
            if not monday_api_key.value:
                raise RuntimeError("Monday API key not found in Vault")
            roster = await get_monday_roster(monday_api_key.value, webhookEvent)
            logging.info(f"Got {len(roster)} users from Monday.com")
            return roster


@app.activity_trigger(input_name="roster")
async def push_to_aad(roster: list[MondayUser]) -> list:
    email_to_user_id = await msgraph_fns.upsert_aad_users(roster)
    await msgraph_fns.sync_all_group_membership(roster, email_to_user_id)
    return []  # Durable Function runtime requires a JSON-parsable return value


@app.activity_trigger(input_name="roster")
async def push_to_gaggle(roster: list[MondayUser]) -> list:
    async with DefaultAzureCredential() as credential:
        async with SecretClient(vault_url=AZURE_VAULT_URL, credential=credential) as secret_client:
            gaggle_api_key = await secret_client.get_secret(GAGGLE_SECRET_NAME)
            if not gaggle_api_key.value:
                raise RuntimeError("Gaggle API key not found in Vault")
    await gaggle_fns.sync_all_group_membership(gaggle_api_key.value, roster)
    return []
