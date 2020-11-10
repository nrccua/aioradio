'''Generic functions related to Jira.'''

from typing import Any
from typing import Dict

import httpx


async def post_jira_issue(url: str, jira_user: str, jira_token: str, payload: Dict[str, Any]):
    '''Post payload to create jira issue.'''

    headers = {'Content-Type': 'application/json'}
    auth = (jira_user, jira_token)
    async with httpx.AsyncClient() as client:
        return await client.post(url=url, json=payload, auth=auth, headers=headers)


async def get_jira_issue(url: str, jira_user: str, jira_token: str):
    '''Get Jira issue using jira_link built with the expected jira_id,
    an example: https://nrccua.atlassian.net/rest/api/2/issue/<jira_id>.'''

    headers = {'Content-Type': 'application/json'}
    auth = (jira_user, jira_token)
    async with httpx.AsyncClient() as client:
        return await client.get(url=url, auth=auth, headers=headers)


async def add_comment_to_jira(url: str, jira_user: str, jira_token: str, comment: str):
    '''Add Jira comment to an existing issue.'''

    if not url.endswith('comment'):
        msg = 'Check url value! Good example is https://nrccua.atlassian.net/rest/api/2/issue/<jira_id>/comment'
        raise ValueError(msg)

    return await post_jira_issue(
        url=url, payload={'body': comment}, jira_user=jira_user, jira_token=jira_token)
