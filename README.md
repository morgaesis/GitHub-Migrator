# GitHub Repo Transporter 🚚

A no-nonsense, idempotent Python script to migrate a GitHub repository and its associated Project (V2)
between organizations. It moves code, history, labels, milestones, issues, and project data.

Built to be run, re-run, and trusted.

## ✨ Key Features

- **Mirror-Perfect Git Data:** Uses `git clone --mirror` for a flawless copy of all branches and tags.
- **Full Metadata Migration:** Replicates labels, milestones, issues, comments, and
  project data using the GraphQL API.
- **Idempotent & Reconcilable:** Run it again to pick up where you left off or
  use the `--reconcile-repo` flag to sync changes to already-migrated issues.
- **Robust & Resilient:** Includes automatic retries with exponential backoff to handle API rate limiting.
- **Preserves History:** Original authors and creation dates are embedded in the migrated issue and comment bodies.

---

## ⚙️ Setup

1. **Dependencies:** Make sure you have Python 3.8+ and install the required library.

   ```bash
   pip install -r ./requirements.txt
   ```

2. **Personal Access Tokens (PATs):** You need two "classic" PATs with the correct scopes.

   - **`SOURCE_TOKEN`**: Needs `repo` and `read:project`.
   - **`TARGET_TOKEN`**: Needs `repo`, `workflow`, and `write:project`.

3. **Configuration:** The script works with a configuration file, environment variables, or CLI flags.
   For file-based configuration, create `~/.config/github-migrator/config.ini`:

   ```ini
   [GITHUB]
   SOURCE_TOKEN = ghp_YourSourceTokenGoesHere...
   TARGET_TOKEN = ghp_YourTargetTokenGoesHere...

   [SOURCE]
   ORG  = source-organization-name
   REPO = the-repo-to-migrate

   [TARGET]
   ORG  = target-organization-name
   REPO = new-repo-name # Can be different

   [PROJECT]
   SOURCE_PROJECT_NAME = "Source Project V2 Name"
   TARGET_PROJECT_NAME = "Target Project V2 Name"
   ```

---

## 🚀 Usage

The script is controlled via flags. Here are the main use cases:

**1. Perform a full, one-time migration:**

```bash
python github-migrator.py --repo --project
```

**2. Update a previous migration (syncs issue states and project board):**

```bash
python github-migrator.py --reconcile-repo --project
```

Use `-v` for more detailed logging or `-vv` for debugging.

---

## 🚨 API Limitations & Caveats

- **Authorship:** The user owning the `TARGET_TOKEN` will be the creator of all issues and comments.
  This is a GitHub API limitation.
  The original author is noted in the body text.
- **Pull Requests:** Migrated as issues to preserve the discussion and history.
  The code branches are mirrored perfectly.
- **Project Views:** The project _data_ (fields, items, statuses) is migrated, but
  the _views_ (Board, Table layouts) are **not**. This is a limitation of the GitHub API.
  You must recreate them manually in the UI.
