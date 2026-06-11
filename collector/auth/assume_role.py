from __future__ import annotations
import asyncio
import os
import re
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
import boto3
from botocore.exceptions import NoCredentialsError, ProfileNotFound
from rich.console import Console

console = Console()

class AssumeRoleAuth:
    def __init__(
        self,
        role_arn: str,
        external_id: str,
        session_name: str = "4cast-collector",
        source_profile: str = "",
        duration_seconds: int = 3600,
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        aws_session_token: str = "",
    ) -> None:
        self.role_arn = role_arn.strip()
        self.external_id = external_id.strip()
        self.session_name = session_name.strip() or "4cast-collector"
        self.source_profile = source_profile.strip()
        self.duration_seconds = duration_seconds
        self._prebuilt_access_key = aws_access_key_id.strip()
        self._prebuilt_secret_key = aws_secret_access_key.strip()
        self._prebuilt_session_token = aws_session_token.strip()

    async def get_accounts(self) -> list[dict]:
        return await asyncio.to_thread(self._get_accounts_sync)

    def _get_accounts_sync(self) -> list[dict]:
        worker_mode = bool(self._prebuilt_access_key)
        try:
            if worker_mode:
                management_creds = {
                    "AccessKeyId": self._prebuilt_access_key,
                    "SecretAccessKey": self._prebuilt_secret_key,
                    "SessionToken": self._prebuilt_session_token,
                }
                management_session = self._session_from_creds(management_creds)
                source_sts = management_session.client("sts")
            else:
                source_session = self._source_session()
                source_sts = source_session.client("sts")
                management_creds = self._assume_role(source_sts, self.role_arn)
                management_session = self._session_from_creds(management_creds)
        except Exception as exc:
            console.print(f"[red]✗[/red] AssumeRole failed for {self.role_arn}: {exc}")
            return []

        identity = management_session.client("sts").get_caller_identity()
        account_id = identity["Account"]
        effective_role_arn = self.role_arn or self._role_arn_from_identity(identity.get("Arn", ""))
        account_name = self._account_name(management_session, account_id)

        org_accounts = self._organization_accounts(management_session)
        if not org_accounts:
            return [
                self._account_entry(
                    account_id,
                    account_name,
                    effective_role_arn,
                    management_creds,
                    refresh_credentials=self._refresh_callback(
                        source_sts=source_sts,
                        role_arn=effective_role_arn,
                        use_external_id=not worker_mode,
                    ) if not worker_mode else None,
                )
            ]

        accounts = []
        role_name = self._role_name(effective_role_arn)
        for org_account in org_accounts:
            org_account_id = org_account["id"]
            org_account_name = org_account["name"]
            if org_account_id == account_id:
                accounts.append(self._account_entry(org_account_id, org_account_name, effective_role_arn, management_creds, email=org_account.get("email")))
                continue
            
            member_role_arn = f"arn:aws:iam::{org_account_id}:role/{role_name}"
            try:
                member_creds = self._assume_role(source_sts, member_role_arn, use_external_id=not worker_mode)
                accounts.append(self._account_entry(org_account_id, org_account_name, member_role_arn, member_creds, email=org_account.get("email")))
            except Exception:
                continue
        return accounts

    def _source_session(self) -> boto3.Session:
        return boto3.Session(profile_name=self.source_profile) if self.source_profile else boto3.Session()

    def _assume_role(self, sts, role_arn: str, use_external_id: bool = True) -> dict:
        params = {"RoleArn": role_arn, "RoleSessionName": self.session_name, "DurationSeconds": self.duration_seconds}
        if use_external_id: params["ExternalId"] = self.external_id
        return sts.assume_role(**params)["Credentials"]

    def _refresh_callback(self, *, source_sts, role_arn: str, use_external_id: bool) -> Callable[[], dict[str, Any]]:
        return lambda: self._assume_role(source_sts, role_arn, use_external_id=use_external_id)

    def _session_from_creds(self, creds: dict) -> boto3.Session:
        return boto3.Session(aws_access_key_id=creds["AccessKeyId"], aws_secret_access_key=creds["SecretAccessKey"], aws_session_token=creds["SessionToken"])

    def _account_entry(self, account_id: str, account_name: str, role_arn: str, creds: dict, email: str | None = None, refresh_credentials = None) -> dict:
        entry = {"id": account_id, "name": account_name, "role_arn": role_arn, "access_key_id": creds["AccessKeyId"], "secret_access_key": creds["SecretAccessKey"], "session_token": creds["SessionToken"], "credential_expires_at": str(creds.get("Expiration", ""))}
        if refresh_credentials: entry["_refresh_credentials"] = refresh_credentials
        return entry

    def _organization_accounts(self, session: boto3.Session) -> list[dict]:
        try:
            org = session.client("organizations")
            return [{"id": a["Id"], "name": a.get("Name", a["Id"])} for page in org.get_paginator("list_accounts").paginate() for a in page.get("Accounts", []) if a.get("Status") == "ACTIVE"]
        except Exception: return []

    def _account_name(self, session: boto3.Session, account_id: str) -> str:
        try: return session.client("organizations").describe_account(AccountId=account_id)["Account"].get("Name", account_id)
        except Exception: return account_id

    def _role_arn_from_identity(self, identity_arn: str) -> str:
        match = re.search(r"assumed-role/([^/]+)/", identity_arn)
        return match.group(1) if match else identity_arn

    def _role_name(self, role_arn: str) -> str:
        match = re.search(r":role/(.+)$", role_arn)
        return match.group(1) if match else role_arn
