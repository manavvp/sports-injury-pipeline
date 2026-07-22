# =============================================================================
# GitHub Actions OIDC Federation
#
# Establishes trust between GitHub's OIDC provider and this AWS account, and
# defines two roles that workflows can assume via short-lived credentials —
# no long-lived access keys in GitHub Secrets.
#
#   - GitHubActionsCI: assumed by pull_request workflows. Read-only + state lock.
#   - GitHubActionsCD: assumed by pushes to main. Full CRUD on project resources.
#
# Trust policies scope by the GitHub `sub` claim so only workflows in this
# repo, in the allowed contexts, can assume these roles.
# =============================================================================

locals {
  github_repo = "manavvp/sports-injury-pipeline"
}

# -----------------------------------------------------------------------------
# OIDC provider — the trust anchor. One provider serves any number of roles.
# -----------------------------------------------------------------------------
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]

  # AWS no longer validates thumbprints for well-known OIDC providers, but the
  # argument is still required by the API. Both current GitHub thumbprints.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]

  lifecycle {
    prevent_destroy = true
  }
}

# -----------------------------------------------------------------------------
# Trust policies — Condition blocks lock down who can assume each role.
# Without the sub-claim condition, any GitHub workflow anywhere could assume.
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "github_ci_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # PR context only — CI runs on pull_request events
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${local.github_repo}:pull_request"]
    }
  }
}

data "aws_iam_policy_document" "github_cd_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Main branch pushes only — CD runs on merge to main
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${local.github_repo}:ref:refs/heads/main"]
    }
  }
}

# -----------------------------------------------------------------------------
# Roles
# -----------------------------------------------------------------------------
resource "aws_iam_role" "github_ci" {
  name               = "GitHubActionsCI"
  assume_role_policy = data.aws_iam_policy_document.github_ci_assume.json

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role" "github_cd" {
  name               = "GitHubActionsCD"
  assume_role_policy = data.aws_iam_policy_document.github_cd_assume.json

  lifecycle {
    prevent_destroy = true
  }
}

# -----------------------------------------------------------------------------
# CI permissions
#
# `terraform plan` needs:
#   - Read on all managed resources (to refresh state)          → ReadOnlyAccess
#   - Read/write on the state bucket (for lockfile + state read) → custom below
# -----------------------------------------------------------------------------
resource "aws_iam_role_policy_attachment" "github_ci_readonly" {
  role       = aws_iam_role.github_ci.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

data "aws_iam_policy_document" "github_ci_state" {
  # S3 native locking creates/deletes a .tflock object on lock acquire/release.
  # Even `plan` acquires the lock by default.
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["arn:aws:s3:::sports-injury-pipeline-manav-tfstate/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::sports-injury-pipeline-manav-tfstate"]
  }
}

resource "aws_iam_role_policy" "github_ci_state" {
  name   = "TerraformStateAccess"
  role   = aws_iam_role.github_ci.id
  policy = data.aws_iam_policy_document.github_ci_state.json
}

# -----------------------------------------------------------------------------
# CD permissions
#
# `terraform apply` needs full CRUD on managed resources. PowerUserAccess
# covers everything except IAM; a custom policy scopes IAM ops to this
# project's roles + OIDC provider, plus PassRole for GlueServiceRole
# (required when Glue job resources reference the role).
# -----------------------------------------------------------------------------
resource "aws_iam_role_policy_attachment" "github_cd_poweruser" {
  role       = aws_iam_role.github_cd.name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

data "aws_iam_policy_document" "github_cd_iam" {
  # Manage this project's IAM roles (Glue service role + our own CI/CD roles).
  # ARN-scoped so this role can't touch unrelated roles in the account.
  statement {
    effect = "Allow"
    actions = [
      "iam:GetRole",
      "iam:UpdateRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRolePolicy",
      "iam:PutRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:AttachRolePolicy",
      "iam:DetachRolePolicy",
    ]
    resources = [
      aws_iam_role.glue_service_role.arn,
      aws_iam_role.github_ci.arn,
      aws_iam_role.github_cd.arn,
    ]
  }

  # Manage the OIDC provider itself (thumbprint/client-id updates, tags).
  # Deliberately excludes Create/Delete — those are bootstrap operations.
  statement {
    effect = "Allow"
    actions = [
      "iam:GetOpenIDConnectProvider",
      "iam:UpdateOpenIDConnectProviderThumbprint",
      "iam:AddClientIDToOpenIDConnectProvider",
      "iam:RemoveClientIDFromOpenIDConnectProvider",
      "iam:TagOpenIDConnectProvider",
      "iam:UntagOpenIDConnectProvider",
    ]
    resources = [aws_iam_openid_connect_provider.github.arn]
  }

  # PassRole — required when Terraform creates/updates Glue jobs that
  # reference GlueServiceRole. Scoped to that role only; this identity
  # cannot pass any other role.
  statement {
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.glue_service_role.arn]
  }
}

resource "aws_iam_role_policy" "github_cd_iam" {
  name   = "TerraformIAMManagement"
  role   = aws_iam_role.github_cd.id
  policy = data.aws_iam_policy_document.github_cd_iam.json
}
