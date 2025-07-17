#!/usr/bin/env python3
import configparser
import logging
import os
import random
import subprocess
import sys
import time
from github import Github, GithubException, RateLimitExceededException

# --- Configuration for Retry Logic ---
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 2  # Initial delay for backoff

# --- Basic Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


# NEW: The robust API call executor
def execute_with_retries(api_call, *args, **kwargs):
    """
    Executes a PyGithub API call with exponential backoff and jitter for rate limiting.
    """
    for attempt in range(MAX_RETRIES):
        try:
            return api_call(*args, **kwargs)
        except RateLimitExceededException:
            delay = (BASE_DELAY_SECONDS**attempt) + random.uniform(0, 1)
            logging.warning(
                f"Rate limit exceeded for call '{api_call.__name__}'. "
                f"Attempt {attempt + 1}/{MAX_RETRIES}. Retrying in {delay:.2f} seconds..."
            )
            time.sleep(delay)
        except GithubException as e:
            # Handle other potential GitHub API errors, not just rate limiting
            logging.error(
                f"A GitHub API error occurred during '{api_call.__name__}': {e.status} {e.data}"
            )
            # Returning None signals a failure to the calling logic
            return None

    logging.error(
        f"API call '{api_call.__name__}' failed after {MAX_RETRIES} retries. Giving up on this item."
    )
    return None


# --- ALL OTHER FUNCTIONS are modified to use `execute_with_retries` ---


def get_github_instance(token):
    # ... (no changes from previous script)
    try:
        g = Github(token)
        user = g.get_user()
        logging.info(f"Successfully authenticated to GitHub as: {user.login}")
        return g
    except Exception as e:
        logging.error(f"Failed to authenticate with token. Error: {e}")
        sys.exit(1)


def get_or_create_repo(g_target, target_org_name, target_repo_name, source_repo):
    # ... (no changes from previous script)
    try:
        org = g_target.get_organization(target_org_name)
    except GithubException:
        logging.error(
            f"Target organization '{target_org_name}' not found or token lacks permission."
        )
        sys.exit(1)

    try:
        target_repo = org.get_repo(target_repo_name)
        logging.info(
            f"Target repo '{target_org_name}/{target_repo_name}' already exists. Reconciling."
        )
    except GithubException:
        logging.info(
            f"Target repo '{target_org_name}/{target_repo_name}' not found. Creating it."
        )
        repo_params = {
            "name": target_repo_name,
            "description": f"Migrated from {source_repo.full_name}. {source_repo.description or ''}",
            "homepage": source_repo.homepage,
            "private": source_repo.private,
            "has_issues": True,
            "has_projects": False,
            "has_wiki": source_repo.has_wiki,
        }
        target_repo = execute_with_retries(org.create_repo, **repo_params)
        if not target_repo:
            logging.critical(
                "Failed to create target repository after all retries. Aborting."
            )
            sys.exit(1)

        logging.info("Repo created successfully.")
    return target_repo


def mirror_git_repository(source_url, target_url):
    # ... (no changes from previous script)
    repo_dir = f"{source_url.split('/')[-1]}.git"
    logging.info("Step 1: Mirroring the Git repository. This might take a while...")
    if not os.path.exists(repo_dir):
        logging.info(f"Cloning mirror from {source_url}...")
        subprocess.run(
            ["git", "clone", "--mirror", source_url, repo_dir],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        logging.info(f"Local mirror '{repo_dir}' already exists. Fetching updates...")
        subprocess.run(
            ["git", "remote", "update"],
            check=True,
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
    logging.info(f"Pushing mirror to {target_url}...")
    try:
        subprocess.run(
            ["git", "push", "--mirror", target_url],
            check=True,
            cwd=repo_dir,
            capture_output=True,
            text=True,
        )
        logging.info("✅ Git repository mirrored successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Failed to push mirror. This often happens if the target repo is not empty. Git command failed with error: {e.stderr}"
        )


def reconcile_labels(source_repo, target_repo):
    logging.info("Step 2: Reconciling labels...")
    source_labels = {label.name.lower(): label for label in source_repo.get_labels()}
    target_labels = {label.name.lower(): label for label in target_repo.get_labels()}

    for name, source_label in source_labels.items():
        sanitized_name = "".join(
            c
            for c in source_label.name
            if c.isalnum() or c.isspace() or c in "😀😁😂🤣😃😄😅😆😉😊😋😎😍😘"
        )

        if name in target_labels:
            target_label = target_labels[name]
            if (
                target_label.color != source_label.color
                or target_label.description != source_label.description
            ):
                logging.info(f"Updating label '{sanitized_name}'")
                execute_with_retries(
                    target_label.edit,
                    name=sanitized_name,
                    color=source_label.color,
                    description=source_label.description or "",
                )
        else:
            logging.info(f"Creating label '{sanitized_name}'")
            execute_with_retries(
                target_repo.create_label,
                name=sanitized_name,
                color=source_label.color,
                description=source_label.description or "",
            )
    logging.info("✅ Labels reconciled.")


def reconcile_milestones(source_repo, target_repo):
    logging.info("Step 3: Reconciling milestones...")
    source_milestones = {ms.title: ms for ms in source_repo.get_milestones(state="all")}
    target_milestones = {ms.title: ms for ms in target_repo.get_milestones(state="all")}
    milestone_map = {}

    for title, source_ms in source_milestones.items():
        if title in target_milestones:
            logging.info(f"Milestone '{title}' exists. Updating.")
            target_ms = target_milestones[title]
            execute_with_retries(
                target_ms.edit,
                title=source_ms.title,
                state=source_ms.state,
                description=source_ms.description or "",
                due_on=source_ms.due_on,
            )
            milestone_map[source_ms.number] = target_ms
        else:
            logging.info(f"Creating milestone '{title}'")
            new_ms = execute_with_retries(
                target_repo.create_milestone,
                title=source_ms.title,
                state=source_ms.state,
                description=source_ms.description or "",
                due_on=source_ms.due_on,
            )
            if new_ms:
                milestone_map[source_ms.number] = new_ms
    logging.info("✅ Milestones reconciled.")
    return milestone_map


def build_migrated_issue_map(target_repo, source_repo_full_name):
    # ... (no changes from previous script)
    logging.info("Building map of already-migrated issues for idempotency check...")
    id_map = {}  # source_issue_number -> target_issue_object
    marker_text = f"Migrated from {source_repo_full_name}#"
    for issue in target_repo.get_issues(state="all"):
        if marker_text in issue.body:
            try:
                original_num_str = issue.body.split(marker_text)[1].split("\n")[0]
                original_num = int(original_num_str)
                id_map[original_num] = issue
            except (IndexError, ValueError):
                continue
    logging.info(f"Found {len(id_map)} issues already migrated.")
    return id_map


def reconcile_issues(source_repo, target_repo, milestone_map, member_logins):
    """The main event. Time to play God with Git history... carefully."""
    logging.info("Step 4: Reconciling issues and pull requests...")

    migrated_issue_map = build_migrated_issue_map(target_repo, source_repo.full_name)
    all_source_issues = source_repo.get_issues(
        state="all", sort="created", direction="asc"
    )

    for source_issue in all_source_issues:
        if source_issue.number in migrated_issue_map:
            logging.info(
                f"Skipping source issue #{source_issue.number} - already migrated to target issue #{migrated_issue_map[source_issue.number].number}"
            )
            continue

        logging.info(
            f"--- Migrating source issue #{source_issue.number}: '{source_issue.title}' ---"
        )

        # --- Build the new issue body with metadata ---
        migration_marker = (
            f"Migrated from {source_repo.full_name}#{source_issue.number}"
        )
        original_author = f"**Original author: @{source_issue.user.login}**"
        original_date = f"**Created at: {source_issue.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}**"
        pr_info = (
            "\n\n*Note: This was a pull request. The code and branches have been mirrored.*"
            if source_issue.pull_request
            else ""
        )
        new_body = f"{migration_marker}\n{original_author}\n{original_date}\n\n---\n\n{source_issue.body or ''}{pr_info}"

        # --- THE FIX: Build a dictionary of parameters dynamically ---
        target_milestone = (
            milestone_map.get(source_issue.milestone.number)
            if source_issue.milestone
            else None
        )

        issue_params = {
            "title": source_issue.title,
            "body": new_body,
            "labels": [label.name for label in source_issue.labels],
            "assignees": [
                a.login for a in source_issue.assignees if a.login in member_logins
            ],
        }

        # Only add the milestone if it's not None
        if target_milestone:
            issue_params["milestone"] = target_milestone

        # --- Create the new issue using our robust wrapper and the dynamic parameters ---
        target_issue = execute_with_retries(target_repo.create_issue, **issue_params)
        # --- END OF FIX ---

        if not target_issue:
            logging.error(
                f"Failed to create issue for source #{source_issue.number} after all retries. Skipping."
            )
            continue

        logging.info(
            f"Successfully created target issue #{target_issue.number} for source #{source_issue.number}"
        )

        # --- Migrate comments ---
        for comment in source_issue.get_comments():
            comment_body = f"**Original author: @{comment.user.login}**\n**Created at: {comment.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}**\n\n---\n\n{comment.body}"
            result = execute_with_retries(target_issue.create_comment, comment_body)
            if result:
                logging.info(
                    f"  > Added comment from @{comment.user.login} to target issue #{target_issue.number}"
                )

        # --- Sync the state (open/closed) ---
        if source_issue.state == "closed":
            logging.info(
                f"Source issue #{source_issue.number} was closed. Closing target issue #{target_issue.number}."
            )
            execute_with_retries(target_issue.edit, state="closed")

    logging.info("✅ Issues and pull requests reconciled.")


def main():
    config = configparser.ConfigParser()
    path = os.path.expanduser("~/.config/github-migrator/config.ini")
    config.read(path)

    try:
        source_token = config["GITHUB"]["SOURCE_TOKEN"]
        target_token = config["GITHUB"]["TARGET_TOKEN"]
        source_org_name = config["SOURCE"]["ORG"]
        source_repo_name = config["SOURCE"]["REPO"]
        target_org_name = config["TARGET"]["ORG"]
        target_repo_name = config["TARGET"]["REPO"]
    except KeyError as e:
        logging.error(f"Configuration error: Missing key {e} in config.ini")
        sys.exit(1)

    g_source = get_github_instance(source_token)
    g_target = get_github_instance(target_token)

    try:
        source_repo = g_source.get_repo(f"{source_org_name}/{source_repo_name}")
        logging.info(f"Found source repo: {source_repo.full_name}")
    except GithubException:
        logging.error(
            f"Source repo '{source_org_name}/{source_repo_name}' not found or token lacks permission."
        )
        sys.exit(1)

    target_repo = get_or_create_repo(
        g_target, target_org_name, target_repo_name, source_repo
    )

    source_url = f"https://{g_source.get_user().login}:{source_token}@github.com/{source_repo.full_name}.git"
    target_url = f"https://{g_target.get_user().login}:{target_token}@github.com/{target_repo.full_name}.git"

    mirror_git_repository(source_url, target_url)
    reconcile_labels(source_repo, target_repo)
    milestone_map = reconcile_milestones(source_repo, target_repo)

    try:
        target_org = g_target.get_organization(target_org_name)
        member_logins = {member.login for member in target_org.get_members()}
    except GithubException:
        logging.warning(
            f"Could not fetch members for target org '{target_org_name}'. Assignees may not be matched."
        )
        member_logins = set()

    reconcile_issues(source_repo, target_repo, milestone_map, member_logins)

    logging.info("🎉 Migration script finished successfully! Go check your new repo.")
    logging.info(f"URL: https://github.com/{target_repo.full_name}")


if __name__ == "__main__":
    main()
