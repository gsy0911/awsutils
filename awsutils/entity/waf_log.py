import json
from dataclasses import dataclass
from typing import List

import s3fs

fs = s3fs.S3FileSystem(anon=False)


@dataclass(frozen=True)
class WafLogHttpRequest:
    args: dict
    client_ip: str
    country: str
    headers: list
    http_method: str
    http_version: str
    request_id: str
    uri: str

    @staticmethod
    def of(log: dict):
        return WafLogHttpRequest(
            args=log["args"],
            client_ip=log["clientIp"],
            country=log["country"],
            headers=log["headers"],
            http_method=log["httpMethod"],
            http_version=log["httpVersion"],
            request_id=log["requestId"],
            uri=log["uri"],
        )


@dataclass(frozen=True)
class WafLog:
    """
    See Also:
        https://docs.aws.amazon.com/waf/latest/developerguide/logging.html
    """

    action: str
    format_version: str
    http_request: WafLogHttpRequest
    timestamp: str
    http_source_id: str
    http_source_name: str
    non_terminating_matching_rules: list
    request_headers_inserted: str
    rate_based_rule_list: list
    response_code_sent: str
    rule_group_list: str
    terminating_rule_id: str
    terminating_rule_match_details: str
    terminating_rule_type: str
    web_acl_id: str

    @staticmethod
    def of(log: dict):
        return WafLog(
            action=log["action"],
            format_version=log["formatVersion"],
            http_request=WafLogHttpRequest.of(log=log["httpRequest"]),
            timestamp=log["timestamp"],
            http_source_id=log["httpSourceId"],
            http_source_name=log["httpSourceName"],
            non_terminating_matching_rules=log["nonTerminatingMatchingRules"],
            rate_based_rule_list=log["rateBasedRuleList"],
            request_headers_inserted=log["requestHeadersInserted"],
            response_code_sent=log["responseCodeSent"],
            rule_group_list=log["ruleGroupList"],
            terminating_rule_id=log["terminatingRuleId"],
            terminating_rule_match_details=log["terminatingRuleMatchDetails"],
            terminating_rule_type=log["terminatingRuleType"],
            web_acl_id=log["webaclId"],
        )

    @staticmethod
    def from_gzip(file_name) -> List["WafLog"]:
        log_list = []
        with fs.open(file_name, "r") as f:
            for line in f.readlines():
                log_list.append(WafLog.of(json.loads(line)))

        return log_list
