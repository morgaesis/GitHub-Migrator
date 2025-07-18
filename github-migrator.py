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

# --- GraphQL Statements ---
# Why group all GraphQL here? Centralizing these complex strings makes them
# easier to find, manage, and debug apart from the Python logic.

GET_VIEWER_LOGIN_QUERY = """query { viewer { login } }"""
GET_REPO_OWNER_DATA_QUERY = """query GetRepoData($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { id, owner { id }, isPrivate } }"""
CREATE_REPO_MUTATION = """mutation CreateRepo($ownerId: ID!, $name: String!, $visibility: RepositoryVisibility!, $description: String) { createRepository(input: {ownerId: $ownerId, name: $name, visibility: $visibility, description: $description}) { repository { id } } }"""
GET_LABELS_QUERY = """query GetLabels($owner: String!, $name: String!, $cursor: String) { repository(owner: $owner, name: $name) { labels(first: 100, after: $cursor) { pageInfo { hasNextPage, endCursor }, nodes { id, name, color, description } } } }"""
CREATE_LABEL_MUTATION = """mutation CreateLabel($repoId: ID!, $name: String!, $color: String!, $description: String) { createLabel(input: {repositoryId: $repoId, name: $name, color: $color, description: $description}) { label { id } } }"""
UPDATE_LABEL_MUTATION = """mutation UpdateLabel($id: ID!, $name: String!, $color: String!, $description: String) { updateLabel(input: {id: $id, name: $name, color: $color, description: $description}) { label { id } } }"""
GET_MILESTONES_QUERY = """query GetMilestones($owner: String!, $name: String!, $cursor: String) { repository(owner: $owner, name: $name) { milestones(first: 100, after: $cursor, states: [OPEN, CLOSED]) { pageInfo { hasNextPage, endCursor }, nodes { id, number, title, state, description, dueOn } } } }"""
CREATE_MILESTONE_MUTATION = """mutation CreateMilestone($repoId: ID!, $title: String!, $description: String, $dueOn: DateTime) { createMilestone(input: {repositoryId: $repoId, title: $title, description: $description, dueOn: $dueOn}) { milestone { id, number } } }"""
UPDATE_MILESTONE_MUTATION = """mutation UpdateMilestone($id: ID!, $title: String, $description: String, $dueOn: DateTime, $state: MilestoneState) { updateMilestone(input: {id: $id, title: $title, description: $description, dueOn: $dueOn, state: $state}) { milestone { id } } }"""
GET_ISSUES_QUERY = """query GetIssues($owner: String!, $name: String!, $cursor: String) { repository(owner: $owner, name: $name) { issues(first: 20, after: $cursor, states: [OPEN, CLOSED], orderBy: {field: CREATED_AT, direction: ASC}) { pageInfo { hasNextPage, endCursor }, nodes { id, number, title, body, state, author { login }, assignees(first: 10) { nodes { login } }, milestone { id }, labels(first: 20) { nodes { name } } } } } }"""
GET_ISSUE_COMMENTS_QUERY = """query GetIssueComments($owner: String!, $name: String!, $number: Int!, $cursor: String) { repository(owner: $owner, name: $name) { issue(number: $number) { comments(first: 100, after: $cursor) { pageInfo { hasNextPage, endCursor }, nodes { author { login }, body } } } } }"""
CREATE_ISSUE_MUTATION = """mutation CreateIssue($repoId: ID!, $title: String!, $body: String, $labelIds: [ID!], $milestoneId: ID) { createIssue(input: {repositoryId: $repoId, title: $title, body: $body, labelIds: $labelIds, milestoneId: $milestoneId}) { issue { id, number } } }"""
ADD_COMMENT_MUTATION = """mutation AddComment($subjectId: ID!, $body: String!) { addComment(input: {subjectId: $subjectId, body: $body}) { commentEdge { node { id } } } }"""
CLOSE_ISSUE_MUTATION = """mutation CloseIssue($issueId: ID!) { closeIssue(input: {issueId: $issueId}) { issue { id } } }"""


class GitHubMigrator:
    """Encapsulates all migration logic, state, and clients."""

    def __init__(self, cli_args):
        self._setup_logging()
        self._load_config_file(cli_args.config)
        self._resolve_configuration(cli_args)
        self._initialize_clients()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
        )

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

        required = [
            "SOURCE_TOKEN",
            "TARGET_TOKEN",
            "SOURCE_ORG",
            "SOURCE_REPO",
            "TARGET_ORG",
            "TARGET_REPO",
        ]
        missing = [key for key in required if not self.cfg.get(key)]
        if missing:
            logging.critical(
                f"Missing required configuration for: {', '.join(missing)}. Provide via CLI, ENV, or config file."
            )
            sys.exit(1)

    def _initialize_clients(self):
        """Initializes the GraphQL API clients and fetches authenticated user logins."""
        self.source_gql = self._GraphQLClient(self.cfg["SOURCE_TOKEN"])
        self.target_gql = self._GraphQLClient(self.cfg["TARGET_TOKEN"])

        # Why fetch user logins? Needed for constructing authenticated git URLs.
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
        """
        for attempt in range(MAX_API_RETRIES):
            try:
                return api_call(*args, **kwargs)
            except requests.exceptions.HTTPError as e:
                # Why check for 403/502? GitHub sometimes returns these for rate-limiting or brief server issues.
                if e.response.status_code not in [403, 429, 502]:
                    raise e  # Re-raise if it's a different, likely permanent, HTTP error.

                delay = (BASE_RETRY_DELAY**attempt) + random.uniform(0, 1)
                logging.warning(
                    f"API call hit a retryable error ({e.response.status_code}). Retrying in {delay:.2f}s..."
                )
                time.sleep(delay)
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}", exc_info=False)
                return None
        logging.error(f"API call failed after {MAX_API_RETRIES} retries.")
        return None

    # --- Repository Migration ---

    def run_repo_migration(self):
        """Orchestrates the entire repository migration using GraphQL for metadata."""
        logging.info("--- Starting Repository Migration ---")

        # Why mirror git first? It's the fastest and most reliable part. Failing early on this saves time.
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

    def _mirror_git_repository(self):
        """
        Uses git's canonical mirror commands for a perfect, high-fidelity
        copy of all refs. This is vastly more reliable and performant than
        any API-based approach for code history.
        """
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
            # Why is this error common? The target repo might not be empty, or the PAT lacks the 'workflow' scope required to push GitHub Actions files.
            logging.error(
                f"Failed to push mirror. Check PAT scopes ('workflow') and ensure target repo is empty. Git error: {e.stderr}"
            )

    def _get_or_create_target_repo(self):
        """Gets the GraphQL ID of the target repo, creating it if it doesn't exist."""
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

        # Why is this check important? It ensures we have the owner ID and visibility from a known-good source repo.
        if not source_repo_data or not source_repo_data.get("repository"):
            logging.critical(
                f"Could not fetch data for source repo to determine owner and visibility."
            )
            return None

        target_owner_data = self._execute_with_retries(
            self.target_gql.execute,
            GET_REPO_OWNER_DATA_QUERY,
            {"owner": self.cfg["TARGET_ORG"], "name": self.cfg["TARGET_REPO"]},
        )
        owner_id = (
            target_owner_data["repository"]["owner"]["id"]
            if target_owner_data and target_owner_data.get("repository")
            else source_repo_data["repository"]["owner"]["id"]
        )

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
        """A generic helper to handle GraphQL cursor-based pagination."""
        nodes = []
        cursor = None
        while True:
            vars_with_cursor = {**variables, "cursor": cursor}
            data = self._execute_with_retries(client.execute, query, vars_with_cursor)
            if not data:
                break

            # Why this structure? It assumes the query has a root object (e.g., 'repository')
            # that contains a paginated connection (e.g., 'labels' or 'issues').
            root_key = list(data.keys())[0]
            connection_key = list(data[root_key].keys())[0]
            connection = data[root_key][connection_key]

            nodes.extend(connection["nodes"])
            if not connection["pageInfo"]["hasNextPage"]:
                break
            cursor = connection["pageInfo"]["endCursor"]
        return nodes

    def _reconcile_items(self, target_repo_id, item_type):
        """
        A generic reconciler for simple items like labels and milestones.
        This reduces code duplication by using a strategy pattern.
        """
        logging.info(f"Reconciling {item_type}...")

        # Define API calls and properties based on the item type
        if item_type == "labels":
            get_query, create_mut, update_mut = (
                GET_LABELS_QUERY,
                CREATE_LABEL_MUTATION,
                UPDATE_LABEL_MUTATION,
            )
            name_key = "name"
        elif item_type == "milestones":
            get_query, create_mut, update_mut = (
                GET_MILESTONES_QUERY,
                CREATE_MILESTONE_MUTATION,
                UPDATE_MILESTONE_MUTATION,
            )
            name_key = "title"
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
            t_item = target_items.get(name)
            variables = {k: v for k, v in s_item.items() if k not in ["id", "number"]}

            if t_item:
                is_diff = any(
                    str(t_item.get(k)) != str(s_item.get(k))
                    for k in variables
                    if k != name_key
                )
                if is_diff:
                    logging.info(f"Updating {item_type[:-1]} '{name}'")
                    self._execute_with_retries(
                        self.target_gql.execute,
                        update_mut,
                        {**variables, "id": t_item["id"]},
                    )
            else:
                logging.info(f"Creating {item_type[:-1]} '{name}'")
                self._execute_with_retries(
                    self.target_gql.execute,
                    create_mut,
                    {**variables, "repoId": target_repo_id},
                )

        # Re-fetch target items to build an accurate ID map for issue creation
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
        """Migrates all issues and their comments."""
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

            # Why embed a marker? This is the key to idempotency. It's how we know we've touched this issue before.
            migration_marker = f"Migrated from {self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}#{s_issue['number']}"
            original_author = f"**Original author: @{s_issue['author']['login'] if s_issue['author'] else 'ghost'}**"
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
                {k: v for k, v in variables.items() if v},
            )
            if not created_issue_data:
                logging.error(
                    f"Failed to create issue for source #{s_issue['number']}. Skipping."
                )
                continue

            t_issue_id = created_issue_data["createIssue"]["issue"]["id"]

            # Migrate comments for the newly created issue
            comments = self._fetch_all_paginated(
                self.source_gql,
                GET_ISSUE_COMMENTS_QUERY,
                {
                    "owner": self.cfg["SOURCE_ORG"],
                    "name": self.cfg["SOURCE_REPO"],
                    "number": s_issue["number"],
                },
            )
            for comment in comments:
                comment_body = f"**Original comment by: @{comment['author']['login'] if comment['author'] else 'ghost'}**\n\n---\n\n{comment['body']}"
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
        """
        Scans the target repository issues for our migration marker to prevent duplicates.
        Returns a dict of {source_issue_number: target_issue_object}.
        """
        id_map = {}
        marker_text = (
            f"Migrated from {self.cfg['SOURCE_ORG']}/{self.cfg['SOURCE_REPO']}#"
        )
        logging.info("Building map of already-migrated issues for idempotency check...")

        target_issues = self._fetch_all_paginated(
            self.target_gql,
            GET_ISSUES_QUERY,
            {"owner": self.cfg["TARGET_ORG"], "name": self.cfg["TARGET_REPO"]},
        )
        for issue in target_issues:
            if issue["body"] and marker_text in issue["body"]:
                try:
                    original_num = int(
                        issue["body"].split(marker_text)[1].split("\n")[0]
                    )
                    id_map[original_num] = issue
                except (IndexError, ValueError):
                    continue
        logging.info(f"Found {len(id_map)} issues already migrated.")
        return id_map

    # --- GraphQL Helper Sub-Class ---
    class _GraphQLClient:
        """A simple, embedded client for executing GitHub GraphQL queries."""

        def __init__(self, token):
            # Why "token"? The "bearer" prefix is standard for OAuth, but "token" is also common for PATs.
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
                raise Exception(
                    f"GraphQL query failed for variables {variables}: {data['errors']}"
                )
            return data.get("data")


def main():
    """Defines the CLI, parses arguments, and orchestrates the migration."""
    default_config_path = os.path.expanduser("~/.config/github-migrator/config.ini")
    os.makedirs(os.path.dirname(default_config_path), exist_ok=True)

    parser = argparse.ArgumentParser(
        description="A tool to migrate a GitHub repository and its Project (V2) board.",
        epilog="Configuration is resolved in order: CLI > Environment Variables > Config File.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # --- Actions ---
    parser.add_argument(
        "--repo",
        action="store_true",
        help="Run the repository migration (issues, labels, etc.).",
    )
    # Project migration can be added here as a separate flag later.

    # --- Configuration Arguments ---
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

    args = parser.parse_args()
    if not args.repo:
        parser.error("No action requested. Please specify --repo.")

    migrator = GitHubMigrator(cli_args=args)
    if args.repo:
        migrator.run_repo_migration()

    logging.info("All requested operations completed successfully.")


if __name__ == "__main__":
    main()
