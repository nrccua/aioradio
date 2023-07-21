"""Pytest Jira."""

# pylint: disable=c-extension-no-member

import os

import pytest

from aioradio.jira import add_comment_to_jira, get_jira_issue, post_jira_issue

pytestmark = pytest.mark.asyncio

JIRA_BASE_URL = 'https://nrccua.atlassian.net/rest/api/2/issue/'
JIRA_ID = 'ARCH-353'
CREDS = {'user': os.getenv('JIRA_USER'), 'pwd': os.getenv('JIRA_PW')}


async def test_post_jira_issue():
    """Test posting Jira issue."""

    pytest.skip("Skip Jira ticket creation as we don't want to create many pointless tickets.")

    payload = {
        "fields": {
            "project": {"key": "ARCH"},
            "issuetype": {"name": "Task"},
            "reporter": {"accountId": "5c1bb94d6158cb25b06fb57d"},
            "priority": {"name": "Medium"},
            "summary": "Pytest ticket creation.  Ticket should be deleted, if not delete manually!",
            "description": "100% test coverage",
            "labels": ["aioradio"],
            "assignee": {"accountId": "5c1bb94d6158cb25b06fb57d"},
            "customfield_10017": "ARCH-306"
        }
    }

    response = await post_jira_issue(url=JIRA_BASE_URL, jira_user=CREDS['user'], jira_token=CREDS['pwd'], payload=payload)
    assert response.status_code == 200
    assert response.json()['key'].startswith('ARCH')


async def test_get_jira_issue():
    """Test getting Jira issue."""

    pytest.skip("Skip get jira issue.")

    response = await get_jira_issue(url=f'{JIRA_BASE_URL}/{JIRA_ID}', jira_user=CREDS['user'], jira_token=CREDS['pwd'])
    assert response.status_code == 200
    assert response.json()['key'] == JIRA_ID


async def test_adding_jira_comment_to_issue():
    """Test adding a comment to a Jira issue."""

    pytest.skip("Skip adding comments to jira ticket.")

    url = f"{JIRA_BASE_URL}/{JIRA_ID}/comment"
    comment = 'pylint-utils Author: [~accountid:5c1bb94d6158cb25b06fb57d]'
    response = await add_comment_to_jira(url=url, jira_user=CREDS['user'], jira_token=CREDS['pwd'], comment=comment)
    assert response.status_code == 201
    assert response.json()
