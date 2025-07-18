#!/usr/bin/env python3

import argparse
import configparser
import logging
import os
import random
import subprocess
import sys
import time
import requests

# --- Global Configuration & Constants ---
GQL_URL = "https://api.github.com/graphql"
MAX_API_RETRIES = 5
BASE_RETRY_DELAY = 2  # seconds


# --- Custom Exception for Actionable Errors ---
class InsufficientScopesError(Exception):
    """Raised when a GraphQL query fails due to missing token scopes."""

    pass


# --- GraphQL Statements ---
ADD_COMMENT_MUTATION = "mutation AddComment($subjectId: ID!, $body: String!) { addComment(input: {subjectId: $subjectId, body: $body}) { commentEdge { node { id } } } }"
ADD_ITEM_TO_PROJECT_MUTATION = "mutation AddItemToProject($projectId: ID!, $contentId: ID!) { addProjectV2ItemById(input: {projectId: $projectId, contentId: $contentId}) { item { id } } }"
CLOSE_ISSUE_MUTATION = "mutation CloseIssue($issueId: ID!) { closeIssue(input: {issueId: $issueId}) { issue { id } } }"
CREATE_FIELD_MUTATION = "mutation CreateField($projectId: ID!, $dataType: ProjectV2FieldType!, $name: String!, $options: [ProjectV2SingleSelectFieldOptionInput!]) { createProjectV2Field(input: { projectId: $projectId, dataType: $dataType, name: $name, singleSelectOptions: $options }) { projectV2Field { ... on ProjectV2Field { id, name } } } }"
CREATE_ISSUE_MUTATION = "mutation CreateIssue($repoId: ID!, $title: String!, $body: String, $labelIds: [ID!], $milestoneId: ID) { createIssue(input: {repositoryId: $repoId, title: $title, body: $body, labelIds: $labelIds, milestoneId: $milestoneId}) { issue { id, number } } }"
CREATE_LABEL_MUTATION = "mutation CreateLabel($repoId: ID!, $name: String!, $color: String!, $description: String) { createLabel(input: {repositoryId: $repoId, name: $name, color: $color, description: $description}) { label { id } } }"
CREATE_MILESTONE_MUTATION = "mutation CreateMilestone($repoId: ID!, $title: String!, $description: String, $dueOn: DateTime) { createMilestone(input: {repositoryId: $repoId, title: $title, description: $description, dueOn: $dueOn}) { milestone { id, number } } }"
CREATE_PROJECT_MUTATION = "mutation CreateProject($ownerId: ID!, $title: String!) { createProjectV2(input: {ownerId: $ownerId, title: $title}) { projectV2 { id } } }"
CREATE_REPO_MUTATION = "mutation CreateRepo($ownerId: ID!, $name: String!, $visibility: RepositoryVisibility!, $description: String) { createRepository(input: {ownerId: $ownerId, name: $name, visibility: $visibility, description: $description}) { repository { id } } }"
GET_ALL_PROJECT_ITEMS_QUERY = "query GetAllProjectItems($projectId: ID!, $cursor: String) { node(id: $projectId) { ... on ProjectV2 { items(first: 100, after: $cursor) { pageInfo { hasNextPage, endCursor }, nodes { id, content { ... on Issue { id, number, repository { nameWithOwner } }, ... on PullRequest { id, number, repository { nameWithOwner } } }, fieldValues(first: 50) { nodes { __typename, ... on ProjectV2ItemFieldTextValue { field { ... on ProjectV2Field { name } }, text }, ... on ProjectV2ItemFieldDateValue { field { ... on ProjectV2Field { name } }, date }, ... on ProjectV2ItemFieldNumberValue { field { ... on ProjectV2Field { name } }, number }, ... on ProjectV2ItemFieldSingleSelectValue { field { ... on ProjectV2Field { name } }, name }, ... on ProjectV2ItemFieldIterationValue { field { ... on ProjectV2Field { name } }, title } } } } } } } }"
GET_ISSUES_QUERY = "query GetIssues($owner: String!, $name: String!, $cursor: String) { repository(owner: $owner, name: $name) { issues(first: 20, after: $cursor, states: [OPEN, CLOSED], orderBy: {field: CREATED_AT, direction: ASC}) { pageInfo { hasNextPage, endCursor }, nodes { id, number, title, body, state, author { login }, assignees(first: 10) { nodes { login } }, milestone { id, number }, labels(first: 20) { nodes { name } }, comments(first: 100) { nodes { author { login }, body, createdAt } } } } } }"
GET_LABELS_QUERY = "query GetLabels($owner: String!, $name: String!, $cursor: String) { repository(owner: $owner, name: $name) { labels(first: 100, after: $cursor) { pageInfo { hasNextPage, endCursor }, nodes { id, name, color, description } } } }"
GET_MILESTONES_QUERY = "query GetMilestones($owner: String!, $name: String!, $cursor: String) { repository(owner: $owner, name: $name) { milestones(first: 100, after: $cursor, states: [OPEN, CLOSED]) { pageInfo { hasNextPage, endCursor }, nodes { id, number, title, state, description, dueOn } } } }"
GET_PROJECT_OWNER_ID_QUERY = (
    "query GetOwnerId($login: String!) { repositoryOwner(login: $login) { id } }"
)
GET_PROJECT_QUERY = "query GetProject($owner: String!, $projectName: String!) { repositoryOwner(login: $owner) { ... on ProjectV2Owner { projectsV2(first: 1, query: $projectName) { nodes { id, title, fields(first: 100) { nodes { __typename, ... on ProjectV2Field { id, name, dataType }, ... on ProjectV2IterationField { id, name, dataType, configuration { iterations { startDate, id } } }, ... on ProjectV2SingleSelectField { id, name, dataType, options { id, name } } } } } } } } }"
GET_REPO_OWNER_DATA_QUERY = "query GetRepoData($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { id, owner { id }, isPrivate } }"
GET_VIEWER_LOGIN_QUERY = "query { viewer { login } }"
UPDATE_ITEM_FIELD_VALUE_MUTATION = "mutation UpdateFieldValue($projectId: ID!, $itemId: ID!, $fieldId: ID!, $value: ProjectV2FieldValue!) { updateProjectV2ItemFieldValue(input: { projectId: $projectId, itemId: $itemId, fieldId: $fieldId, value: $value }) { projectV2Item { id } } }"
UPDATE_LABEL_MUTATION = "mutation UpdateLabel($id: ID!, $name: String!, $color: String!, $description: String) { updateLabel(input: {id: $id, name: $name, color: $color, description: $description}) { label { id } } }"
UPDATE_MILESTONE_MUTATION = "mutation UpdateMilestone($id: ID!, $title: String, $description: String, $dueOn: DateTime, $state: MilestoneState) { updateMilestone(input: {id: $id, title: $title, description: $description, dueOn: $dueOn, state: $state}) { milestone { id } } }"


class GitHubMigrator:
    """Encapsulates all migration logic, state, and clients."""

    def __init__(self, cli_args):
        self._load_config_file(cli_args.config)
        self._resolve_configuration(cli_args)
        self._initialize_clients()

    def _load_config_file(self, config_path):
        """Loads the INI config file. It is not an error for this to be missing."""
        self.config_file = configparser.ConfigParser()
        if os.path.exists(config_path):
            logging.info(f"Loading configuration from '{config_path}'")
            self.config_file.read(config_path)
        else:
            logging.info(
                f"Config file '{config_path}' not found. Relying on CLI args and ENV vars."
            )

    def _resolve_configuration(self, args):
        """
        Resolves all configuration values, with precedence: CLI > ENV > Config file.
        This provides maximum flexibility for different runtime environments.
        """
        self.cfg = {}

        def _get_val(cli_val, env_key, conf_section, conf_key):
            val = cli_val
            if val is not None:
                return val
            val = os.getenv(env_key)
            if val is not None:
                return val
            return self.config_file.get(conf_section, conf_key, fallback=None)

        self.cfg["SOURCE_TOKEN"] = _get_val(
            args.source_token, "GITHUB_SOURCE_TOKEN", "GITHUB", "source_token"
        )
        self.cfg["TARGET_TOKEN"] = _get_val(
            args.target_token, "GITHUB_TARGET_TOKEN", "GITHUB", "target_token"
        )
        self.cfg["SOURCE_ORG"] = _get_val(
            args.source_org, "GITHUB_SOURCE_ORG", "SOURCE", "org"
        )
        self.cfg["SOURCE_REPO"] = _get_val(
            args.source_repo, "GITHUB_SOURCE_REPO", "SOURCE", "repo"
        )
        self.cfg["TARGET_ORG"] = _get_val(
            args.target_org, "GITHUB_TARGET_ORG", "TARGET", "org"
        )
        self.cfg["TARGET_REPO"] = _get_val(
            args.target_repo, "GITHUB_TARGET_REPO", "TARGET", "repo"
        )
        self.cfg["SOURCE_PROJECT_NAME"] = _get_val(
            args.source_project_name,
            "GITHUB_SOURCE_PROJECT_NAME",
            "PROJECT",
            "source_project_name",
        )
        self.cfg["TARGET_PROJECT_NAME"] = _get_val(
            args.target_project_name,
            "GITHUB_TARGET_PROJECT_NAME",
            "PROJECT",
            "target_project_name",
        )

        required_repo = [
            "SOURCE_TOKEN",
            "TARGET_TOKEN",
            "SOURCE_ORG",
            "SOURCE_REPO",
            "TARGET_ORG",
            "TARGET_REPO",
        ]
        required_proj = ["SOURCE_PROJECT_NAME", "TARGET_PROJECT_NAME"]

        missing = []
        if args.repo:
            missing.extend(key for key in required_repo if not self.cfg.get(key))
        if args.project:
            missing.extend(key for key in required_proj if not self.cfg.get(key))

        if missing:
            logging.critical(
                f"Missing required configuration for: {', '.join(sorted(list(set(missing))))}. Provide via CLI, ENV, or config file."
            )
            sys.exit(1)

    def _initialize_clients(self):
        """Initializes the GraphQL API clients and fetches authenticated user logins."""
        self.source_gql = self._GraphQLClient(self.cfg["SOURCE_TOKEN"])
        self.target_gql = self._GraphQLClient(self.cfg["TARGET_TOKEN"])
        self.source_login = self._execute_with_retries(
            self.source_gql.execute, GET_VIEWER_LOGIN_QUERY
        )["viewer"]["login"]
        self.target_login = self._execute_with_retries(
            self.target_gql.execute, GET_VIEWER_LOGIN_QUERY
        )["viewer"]["login"]
        logging.info(f"Source client authenticated as: {self.source_login}")
        logging.info(f"Target client authenticated as: {self.target_login}")

    def _execute_with_retries(self, api_call, *args, **kwargs):
        """
        A wrapper to provide resilience against API rate limiting for any function call.
        It now specifically handles scope errors to provide better user feedback.
        """
        try:
            for attempt in range(MAX_API_RETRIES):
                try:
                    return api_call(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code not in [403, 429, 502]:
                        raise e
                    delay = (BASE_RETRY_DELAY**attempt) + random.uniform(0, 1)
                    logging.warning(
                        f"API call hit a retryable error ({e.response.status_code}). Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
            logging.error(f"API call failed after {MAX_API_RETRIES} retries.")
            return None
        except InsufficientScopesError as e:
            logging.critical("FATAL: GitHub token is missing required permissions.")
            logging.critical(f"API Message: {e}")
            logging.critical(
                "Please ensure your token has the 'repo', 'workflow', 'read:project', and 'write:project' scopes."
            )
            sys.exit(1)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=False)
            return None

    # --- Repository Migration ---
    def run_repo_migration(self):
        logging.info("--- Starting Repository Migration ---")
        self._mirror_git_repository()
        target_repo_id = self._get_or_create_target_repo()
        if not target_repo_id:
            logging.critical("Could not get or create target repo. Aborting.")
            sys.exit(1)
        logging.info(f"Working with target repo ID: {target_repo_id}")
        label_map = self._reconcile_items(target_repo_id, "labels")
        milestone_map = self._reconcile_items(target_repo_id, "milestones")
        self._reconcile_issues(target_repo_id, label_map, milestone_map)
        logging.info("--- Repository Migration Finished ---")
        logging.info(
            f"URL: https://github.com/{self.cfg['TARGET_ORG']}/{self.cfg['TARGET_REPO']}"
        )

    def _mirror_git_repository(self):
        logging.info("Syncing Git data via 'git mirror'...")
        repo_dir = f"{self.cfg['SOURCE_REPO']}.git"
        source_url = f"https://{self.source_login}:{self.cfg['SOURCE_TOKEN']}@github.com/{self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}.git"
        target_url = f"https://{self.target_login}:{self.cfg['TARGET_TOKEN']}@github.com/{self.cfg['TARGET_ORG']}/{self.cfg['TARGET_REPO']}.git"
        if not os.path.exists(repo_dir):
            subprocess.run(
                ["git", "clone", "--mirror", source_url, repo_dir],
                check=True,
                capture_output=True,
            )
        else:
            subprocess.run(
                ["git", "remote", "update"],
                check=True,
                cwd=repo_dir,
                capture_output=True,
            )
        try:
            subprocess.run(
                ["git", "push", "--mirror", target_url],
                check=True,
                cwd=repo_dir,
                capture_output=True,
                text=True,
            )
            logging.info("Git repository mirrored successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(
                f"Failed to push mirror. Check PAT scopes ('workflow') and ensure target repo is empty. Git error: {e.stderr}"
            )

    def _get_or_create_target_repo(self):
        repo_data = self._execute_with_retries(
            self.target_gql.execute,
            GET_REPO_OWNER_DATA_QUERY,
            {"owner": self.cfg["TARGET_ORG"], "name": self.cfg["TARGET_REPO"]},
        )
        if repo_data and repo_data.get("repository"):
            logging.info(f"Target repo '{self.cfg['TARGET_REPO']}' already exists.")
            return repo_data["repository"]["id"]
        logging.info(f"Target repo '{self.cfg['TARGET_REPO']}' not found. Creating...")
        source_repo_data = self._execute_with_retries(
            self.source_gql.execute,
            GET_REPO_OWNER_DATA_QUERY,
            {"owner": self.cfg["SOURCE_ORG"], "name": self.cfg["SOURCE_REPO"]},
        )
        if not source_repo_data or not source_repo_data.get("repository"):
            logging.critical(f"Could not fetch source repo data.")
            return None
        target_owner_data = self._execute_with_retries(
            self.target_gql.execute,
            GET_PROJECT_OWNER_ID_QUERY,
            {"login": self.cfg["TARGET_ORG"]},
        )
        if not target_owner_data or not target_owner_data.get("repositoryOwner"):
            logging.critical(f"Could not resolve target owner ID.")
            return None
        owner_id = target_owner_data["repositoryOwner"]["id"]
        visibility = (
            "PRIVATE" if source_repo_data["repository"]["isPrivate"] else "PUBLIC"
        )
        variables = {
            "ownerId": owner_id,
            "name": self.cfg["TARGET_REPO"],
            "visibility": visibility,
            "description": f"Migrated from {self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}",
        }
        created_data = self._execute_with_retries(
            self.target_gql.execute, CREATE_REPO_MUTATION, variables
        )
        return (
            created_data["createRepository"]["repository"]["id"]
            if created_data
            else None
        )

    def _fetch_all_paginated(self, client, query, variables):
        nodes = []
        cursor = None
        while True:
            vars_with_cursor = {**variables, "cursor": cursor}
            data = self._execute_with_retries(client.execute, query, vars_with_cursor)
            if not data:
                break
            root_key = list(data.keys())[0]
            connection = None
            if data.get(root_key):
                # The top-level key might change (e.g., 'repository', 'node'), but the inner structure
                # of a paginated connection ('nodes', 'pageInfo') is consistent. This handles that variance.
                connection_key = (
                    list(data[root_key].keys())[0] if data[root_key] else None
                )
                if connection_key:
                    connection = data[root_key][connection_key]
            if not connection:
                break
            nodes.extend(connection["nodes"])
            if not connection["pageInfo"]["hasNextPage"]:
                break
            cursor = connection["pageInfo"]["endCursor"]
        return nodes

    def _reconcile_items(self, target_repo_id, item_type):
        logging.info(f"Reconciling {item_type}...")
        if item_type == "labels":
            get_query, create_mut, update_mut, name_key = (
                GET_LABELS_QUERY,
                CREATE_LABEL_MUTATION,
                UPDATE_LABEL_MUTATION,
                "name",
            )
        elif item_type == "milestones":
            get_query, create_mut, update_mut, name_key = (
                GET_MILESTONES_QUERY,
                CREATE_MILESTONE_MUTATION,
                UPDATE_MILESTONE_MUTATION,
                "title",
            )
        else:
            return {}
        source_items_raw = self._fetch_all_paginated(
            self.source_gql,
            get_query,
            {"owner": self.cfg["SOURCE_ORG"], "name": self.cfg["SOURCE_REPO"]},
        )
        target_items_raw = self._fetch_all_paginated(
            self.target_gql,
            get_query,
            {"owner": self.cfg["TARGET_ORG"], "name": self.cfg["TARGET_REPO"]},
        )
        source_items = {item[name_key]: item for item in source_items_raw}
        target_items = {item[name_key]: item for item in target_items_raw}
        for name, s_item in source_items.items():
            variables = {k: v for k, v in s_item.items() if k not in ["id", "number"]}
            if t_item := target_items.get(name):
                # Why do a deep comparison? To achieve idempotency. We only send an update
                # API call if a value has actually changed, saving API quota and time.
                if any(
                    str(t_item.get(k)) != str(s_item.get(k))
                    for k in variables
                    if k != name_key
                ):
                    logging.debug(f"Updating {item_type[:-1]} '{name}'")
                    self._execute_with_retries(
                        self.target_gql.execute,
                        update_mut,
                        {**variables, "id": t_item["id"]},
                    )
            else:
                logging.debug(f"Creating {item_type[:-1]} '{name}'")
                self._execute_with_retries(
                    self.target_gql.execute,
                    create_mut,
                    {**variables, "repoId": target_repo_id},
                )
        refreshed_target_items = self._fetch_all_paginated(
            self.target_gql,
            get_query,
            {"owner": self.cfg["TARGET_ORG"], "name": self.cfg["TARGET_REPO"]},
        )
        logging.info(f"✅ {item_type.capitalize()} reconciled.")
        if item_type == "labels":
            return {item["name"]: item["id"] for item in refreshed_target_items}
        if item_type == "milestones":
            return {item["number"]: item["id"] for item in refreshed_target_items}

    def _reconcile_issues(self, target_repo_id, label_map, milestone_map):
        logging.info("Reconciling issues and comments...")
        migrated_issue_map = self._build_migrated_issue_map()
        source_issues = self._fetch_all_paginated(
            self.source_gql,
            GET_ISSUES_QUERY,
            {"owner": self.cfg["SOURCE_ORG"], "name": self.cfg["SOURCE_REPO"]},
        )
        for s_issue in source_issues:
            if s_issue["number"] in migrated_issue_map:
                continue
            logging.info(
                f"Migrating source issue #{s_issue['number']}: '{s_issue['title']}'"
            )
            migration_marker = f"Migrated from {self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}#{s_issue['number']}"
            original_author = f"**Original author: @{s_issue['author']['login'] if s_issue.get('author') else 'ghost'}**"
            new_body = f"{migration_marker}\n{original_author}\n\n---\n\n{s_issue['body'] or ''}"
            variables = {
                "repoId": target_repo_id,
                "title": s_issue["title"],
                "body": new_body,
                "labelIds": [
                    label_map[label["name"]]
                    for label in s_issue["labels"]["nodes"]
                    if label["name"] in label_map
                ],
                "milestoneId": milestone_map.get(
                    s_issue.get("milestone", {}).get("number")
                )
                if s_issue.get("milestone")
                else None,
            }
            created_issue_data = self._execute_with_retries(
                self.target_gql.execute,
                CREATE_ISSUE_MUTATION,
                {k: v for k, v in variables.items() if v is not None},
            )
            if not created_issue_data:
                logging.error(
                    f"Failed to create issue for source #{s_issue['number']}. Skipping."
                )
                continue
            t_issue_id = created_issue_data["createIssue"]["issue"]["id"]
            # Why migrate comments inside the issue loop? To maintain conversational context and
            # ensure that if issue creation fails, its comments aren't orphaned or missed.
            for comment in sorted(
                s_issue["comments"]["nodes"], key=lambda c: c["createdAt"]
            ):
                comment_body = f"**Original comment by @{comment['author']['login'] if comment.get('author') else 'ghost'} on {comment['createdAt']}**\n\n---\n\n{comment['body']}"
                self._execute_with_retries(
                    self.target_gql.execute,
                    ADD_COMMENT_MUTATION,
                    {"subjectId": t_issue_id, "body": comment_body},
                )
            if s_issue["state"] == "CLOSED":
                self._execute_with_retries(
                    self.target_gql.execute,
                    CLOSE_ISSUE_MUTATION,
                    {"issueId": t_issue_id},
                )

    def _build_migrated_issue_map(self):
        id_map, marker_text = (
            {},
            f"Migrated from {self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}#",
        )
        logging.info("Building map of already-migrated issues for idempotency check...")
        for issue in self._fetch_all_paginated(
            self.target_gql,
            GET_ISSUES_QUERY,
            {"owner": self.cfg["TARGET_ORG"], "name": self.cfg["TARGET_REPO"]},
        ):
            if issue.get("body") and marker_text in issue["body"]:
                try:
                    id_map[int(issue["body"].split(marker_text)[1].split("\n")[0])] = (
                        issue
                    )
                except (IndexError, ValueError):
                    continue
        logging.info(f"Found {len(id_map)} issues already migrated.")
        return id_map

    # --- Project V2 Migration ---
    def run_project_migration(self):
        logging.info("--- Starting Project (V2) Migration ---")
        source_project, target_project = self._get_or_create_target_project()
        target_fields = self._reconcile_project_fields(source_project, target_project)
        self._reconcile_project_items(source_project, target_project, target_fields)
        logging.info("--- Project (V2) Migration Finished ---")

    def _get_or_create_target_project(self):
        s_owner, t_owner = self.cfg["SOURCE_ORG"], self.cfg["TARGET_ORG"]
        s_proj, t_proj = (
            self.cfg["SOURCE_PROJECT_NAME"],
            self.cfg["TARGET_PROJECT_NAME"],
        )
        source_project = self._get_project_data(s_owner, s_proj)
        if not source_project:
            logging.critical("Source project not found. Aborting.")
            sys.exit(1)
        target_project = self._get_project_data(t_owner, t_proj)
        if not target_project:
            logging.info(f"Target project '{t_proj}' not found. Creating...")
            owner_data = self._execute_with_retries(
                self.target_gql.execute, GET_PROJECT_OWNER_ID_QUERY, {"login": t_owner}
            )
            if not owner_data or not owner_data.get("repositoryOwner"):
                logging.critical(f"Could not find owner ID for '{t_owner}'.")
                sys.exit(1)
            owner_id = owner_data["repositoryOwner"]["id"]
            self._execute_with_retries(
                self.target_gql.execute,
                CREATE_PROJECT_MUTATION,
                {"ownerId": owner_id, "title": t_proj},
            )
            target_project = self._get_project_data(
                t_owner, t_proj
            )  # Re-fetch to get the new project's data
        return source_project, target_project

    def _reconcile_project_fields(self, source_project, target_project):
        logging.info("Reconciling project fields...")
        source_fields = {f["name"]: f for f in source_project["fields"]["nodes"]}
        target_fields = {f["name"]: f for f in target_project["fields"]["nodes"]}
        # Why is there a hardcoded list of fields to skip? These are standard, non-customizable
        # fields that are automatically present in every project and cannot be created via the API.
        standard_fields = {
            "Title",
            "Assignees",
            "Status",
            "Labels",
            "Repository",
            "Milestone",
            "Linked pull requests",
        }
        for name, field in source_fields.items():
            if name in target_fields or name in standard_fields:
                continue
            logging.debug(
                f"Creating missing field in target project: '{name}' ({field['dataType']})"
            )
            variables = {
                "projectId": target_project["id"],
                "dataType": field["dataType"],
                "name": name,
                "options": [{"name": opt["name"]} for opt in field.get("options", [])]
                if field["dataType"] == "SINGLE_SELECT"
                else None,
            }
            self._execute_with_retries(
                self.target_gql.execute,
                CREATE_FIELD_MUTATION,
                {k: v for k, v in variables.items() if v is not None},
            )
        refreshed_target_project = self._get_project_data(
            self.cfg["TARGET_ORG"], target_project["title"]
        )
        return {f["name"]: f for f in refreshed_target_project["fields"]["nodes"]}

    def _reconcile_project_items(self, source_project, target_project, target_fields):
        logging.info("Reconciling project items and their field values...")
        # Why build the issue map here again? To ensure we have the most up-to-date
        # mapping between source and target issues, especially if the repo migration
        # just ran in the same session.
        migrated_issue_map = self._build_migrated_issue_map()
        source_item_map = self._build_project_item_map(
            source_project["id"], f"{self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}"
        )
        target_item_map = self._build_project_item_map(
            target_project["id"], f"{self.cfg['TARGET_ORG']}/{self.cfg['TARGET_REPO']}"
        )
        for source_issue_num, s_item_data in source_item_map.items():
            if not (target_issue_obj := migrated_issue_map.get(source_issue_num)):
                continue
            target_issue_num = target_issue_obj["number"]
            if not (t_item_data := target_item_map.get(target_issue_num)):
                logging.info(f"Adding issue #{target_issue_num} to target project...")
                new_item_data = self._execute_with_retries(
                    self.target_gql.execute,
                    ADD_ITEM_TO_PROJECT_MUTATION,
                    {
                        "projectId": target_project["id"],
                        "contentId": target_issue_obj["id"],
                    },
                )
                if not new_item_data:
                    continue
                t_item_data = {
                    "id": new_item_data["addProjectV2ItemById"]["item"]["id"],
                    "fieldValues": {},
                }
            for field_name, source_value in s_item_data["fieldValues"].items():
                if field_name not in target_fields:
                    continue
                if str(t_item_data["fieldValues"].get(field_name)) != str(source_value):
                    logging.debug(
                        f"Updating field '{field_name}' for issue #{target_issue_num} to '{source_value}'"
                    )
                    if not (
                        value_obj := self._get_gql_field_value(
                            target_fields[field_name], source_value
                        )
                    ):
                        continue
                    variables = {
                        "projectId": target_project["id"],
                        "itemId": t_item_data["id"],
                        "fieldId": target_fields[field_name]["id"],
                        "value": value_obj,
                    }
                    self._execute_with_retries(
                        self.target_gql.execute,
                        UPDATE_ITEM_FIELD_VALUE_MUTATION,
                        variables,
                    )

    def _get_project_data(self, owner, project_name):
        data = self._execute_with_retries(
            self.target_gql.execute,
            GET_PROJECT_QUERY,
            {"owner": owner, "projectName": project_name},
        )
        if not data or not data.get("repositoryOwner"):
            return None
        return (
            data["repositoryOwner"]["projectsV2"]["nodes"][0]
            if data["repositoryOwner"]["projectsV2"]["nodes"]
            else None
        )

    def _build_project_item_map(self, project_id, repo_full_name):
        item_map, source_items = (
            {},
            self._fetch_all_paginated(
                self.target_gql, GET_ALL_PROJECT_ITEMS_QUERY, {"projectId": project_id}
            ),
        )
        for item in source_items:
            if (
                not (content := item.get("content"))
                or content.get("repository", {}).get("nameWithOwner") != repo_full_name
            ):
                continue
            # Why list(fv.values())[-1]? This is a shortcut to get the actual value from a
            # ProjectV2ItemFieldValue union type (e.g., text, number, date) without a complex conditional.
            item_map[content["number"]] = {
                "id": item["id"],
                "fieldValues": {
                    fv["field"]["name"]: list(fv.values())[-1]
                    for fv in item["fieldValues"]["nodes"]
                    if fv.get("field")
                },
            }
        return item_map

    def _get_gql_field_value(self, field, value):
        dt = field["dataType"]
        if dt == "TEXT":
            return {"text": str(value)}
        if dt == "NUMBER":
            return {"number": float(value)}
        if dt == "DATE":
            return {"date": str(value)}
        if dt == "SINGLE_SELECT":
            # Why search for the option? The API requires the GraphQL ID of the select option, not its string value.
            if option := next(
                (opt for opt in field.get("options", []) if opt["name"] == value), None
            ):
                return {"singleSelectOptionId": option["id"]}
        return None

    class _GraphQLClient:
        """
        A simple, embedded client for executing GitHub GraphQL queries.
        It now inspects errors to raise specific, actionable exceptions.
        """

        def __init__(self, token):
            self._headers = {"Authorization": f"token {token}"}

        def execute(self, query, variables=None):
            payload = (
                {"query": query, "variables": variables}
                if variables
                else {"query": query}
            )
            response = requests.post(GQL_URL, headers=self._headers, json=payload)
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                first_error = data["errors"][0]
                msg = first_error.get("message", "Unknown GraphQL Error")
                if first_error.get("type") == "INSUFFICIENT_SCOPES":
                    raise InsufficientScopesError(msg)
                else:
                    raise Exception(f"GraphQL query failed: {msg}")
            return data.get("data")


class ColoredFormatter(logging.Formatter):
    """A dependency-free logger formatter that adds ANSI colors to log levels."""

    COLORS = {
        "WARNING": "\033[93m",
        "INFO": "\033[92m",
        "DEBUG": "\033[96m",
        "CRITICAL": "\033[91m",
        "ERROR": "\033[91m",
    }
    RESET = "\033[0m"

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, "")
        record.levelname = f"{log_color}{record.levelname:<8}{self.RESET}"
        return super().format(record)


def setup_logging(level):
    """Configures the root logger with a colored formatter."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = ColoredFormatter("%(levelname)s %(message)s")
    handler.setFormatter(formatter)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(handler)


def main():
    """Defines the CLI, parses arguments, and orchestrates the migration."""
    default_config_path = os.path.expanduser("~/.config/github-migrator/config.ini")
    os.makedirs(os.path.dirname(default_config_path), exist_ok=True)
    parser = argparse.ArgumentParser(
        description="A tool to migrate a GitHub repository and its Project (V2) board.",
        epilog="Configuration is resolved in order: CLI > Environment Variables > Config File.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    verb_group = parser.add_mutually_exclusive_group()
    verb_group.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress INFO logs, showing only warnings and errors.",
    )
    verb_group.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=1,
        help="Increase verbosity. -v for INFO (default), -vv for DEBUG.",
    )
    parser.add_argument(
        "--repo",
        action="store_true",
        help="Run the repository migration (issues, labels, etc.).",
    )
    parser.add_argument(
        "--project",
        action="store_true",
        help="Run the Project (V2) board migration/sync.",
    )
    conf_group = parser.add_argument_group(
        "Configuration (overrides ENV vars and config file)"
    )
    conf_group.add_argument(
        "--config",
        default=default_config_path,
        help=f"Path to the config file (default: {default_config_path})",
    )
    conf_group.add_argument(
        "--source-token", help="Source repo PAT. (ENV: GITHUB_SOURCE_TOKEN)"
    )
    conf_group.add_argument(
        "--target-token", help="Target repo PAT. (ENV: GITHUB_TARGET_TOKEN)"
    )
    conf_group.add_argument(
        "--source-org", help="Source organization/user name. (ENV: GITHUB_SOURCE_ORG)"
    )
    conf_group.add_argument(
        "--source-repo", help="Source repository name. (ENV: GITHUB_SOURCE_REPO)"
    )
    conf_group.add_argument(
        "--target-org", help="Target organization name. (ENV: GITHUB_TARGET_ORG)"
    )
    conf_group.add_argument(
        "--target-repo", help="Target repository name. (ENV: GITHUB_TARGET_REPO)"
    )
    conf_group.add_argument(
        "--source-project-name",
        help="Source Project (V2) name. (ENV: GITHUB_SOURCE_PROJECT_NAME)",
    )
    conf_group.add_argument(
        "--target-project-name",
        help="Target Project (V2) name. (ENV: GITHUB_TARGET_PROJECT_NAME)",
    )
    args = parser.parse_args()

    if args.quiet:
        log_level = logging.WARNING
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    setup_logging(log_level)

    if not args.repo and not args.project:
        parser.error("No action requested. Please specify --repo, --project, or both.")

    try:
        migrator = GitHubMigrator(cli_args=args)
        if args.repo:
            migrator.run_repo_migration()
        if args.project:
            migrator.run_project_migration()
        logging.info("All requested operations completed successfully.")
    except InsufficientScopesError as e:
        # This global catch is a final backstop, even though the retry wrapper handles it.
        logging.critical(
            "Permissions error caught at top level. Please check your PAT scopes."
        )
        sys.exit(1)
    except Exception as e:
        logging.critical(
            f"A fatal, unexpected error occurred: {e}",
            exc_info=log_level == logging.DEBUG,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
