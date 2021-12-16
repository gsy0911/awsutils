import gzip
import shlex
from dataclasses import dataclass
from typing import List

import s3fs

fs = s3fs.S3FileSystem(anon=False)


@dataclass(frozen=True)
class AlbLog:
    """
    See Also:
        https://docs.aws.amazon.com/ja_jp/elasticloadbalancing/latest/application/load-balancer-access-logs.html
    """

    type_: str
    time: str
    elb: str
    client_ip_port: str
    target_ip_port: str
    request_processing_time: str
    target_processing_time: str
    response_processing_time: str
    elb_status_code: str
    target_status_code: str
    received_bytes: str
    sent_bytes: str
    request: str
    user_agent: str
    ssl_cipher: str
    ssl_protocol: str
    target_group_arn: str
    trace_id: str
    domain_name: str
    chosen_cert_arn: str
    matched_rule_policy: str
    actions_executed: str
    redirect_url: str
    error_reason: str
    target_port_list: str
    target_status_code_list: str
    classification: str
    classification_reason: str

    @staticmethod
    def from_gzip(file_name) -> List["AlbLog"]:
        with fs.open(file_name, "rb") as f:
            data = gzip.decompress(f.read())
            log_list = data.decode().split("\n")[:-1]

        return [AlbLog.of(log) for log in log_list]

    @staticmethod
    def of(log: str):
        d = shlex.split(log)

        return AlbLog(
            type_=d[0],
            time=d[1],
            elb=d[2],
            client_ip_port=d[3],
            target_ip_port=d[4],
            request_processing_time=d[5],
            target_processing_time=d[6],
            response_processing_time=d[7],
            elb_status_code=d[8],
            target_status_code=d[9],
            received_bytes=d[10],
            sent_bytes=d[11],
            request=d[12],
            user_agent=d[13],
            ssl_cipher=d[14],
            ssl_protocol=d[15],
            target_group_arn=d[16],
            trace_id=d[17],
            domain_name=d[18],
            chosen_cert_arn=d[19],
            matched_rule_policy=d[20],
            actions_executed=d[21],
            redirect_url=d[22],
            error_reason=d[23],
            target_port_list=d[24],
            target_status_code_list=d[25],
            classification=d[26],
            classification_reason=d[27],
        )

    @staticmethod
    def loads(d: dict):
        return AlbLog(
            type_=d["type"],
            time=d["time"],
            elb=d["elb"],
            client_ip_port=d["client_ip_port"],
            target_ip_port=d["target_ip_port"],
            request_processing_time=d["request_processing_time"],
            target_processing_time=d["target_processing_time"],
            response_processing_time=d["respose_processing_time"],
            elb_status_code=d["elb_status_code"],
            target_status_code=d["target_status_code"],
            received_bytes=d["received_bytes"],
            sent_bytes=d["sent_bytes"],
            request=d["request"],
            user_agent=d["user_agent"],
            ssl_cipher=d["ssl_cipher"],
            ssl_protocol=d["ssl_protocol"],
            target_group_arn=d["target_group_arn"],
            trace_id=d["trace_id"],
            domain_name=d["domain_name"],
            chosen_cert_arn=d["chosen_cert_arn"],
            matched_rule_policy=d["matched_rule_policy"],
            actions_executed=d["actions_executed"],
            redirect_url=d["redirect_url"],
            error_reason=d["error_reason"],
            target_port_list=d["target_port_list"],
            target_status_code_list=d["target_status_code_list"],
            classification=d["classification"],
            classification_reason=d["classification_reason"],
        )

    def dumps(self) -> dict:
        return {
            "type": self.type_,
            "time": self.time,
            "elb": self.elb,
            "client_ip_port": self.client_ip_port,
            "target_ip_port": self.target_ip_port,
            "request_processing_time": self.request_processing_time,
            "target_processing_time": self.target_processing_time,
            "response_processing_time": self.response_processing_time,
            "elb_status_code": self.elb_status_code,
            "target_status_code": self.target_status_code,
            "received_bytes": self.received_bytes,
            "sent_bytes": self.sent_bytes,
            "request": self.request,
            "user_agent": self.user_agent,
            "ssl_cipher": self.ssl_cipher,
            "ssl_protocol": self.ssl_protocol,
            "target_group_arn": self.target_group_arn,
            "trace_id": self.trace_id,
            "domain_name": self.domain_name,
            "chosen_cert_arn": self.chosen_cert_arn,
            "matched_rule_policy": self.matched_rule_policy,
            "actions_executed": self.actions_executed,
            "redirect_url": self.redirect_url,
            "error_reason": self.error_reason,
            "target_port_list": self.target_port_list,
            "target_status_code_list": self.target_status_code_list,
            "classification": self.classification,
            "classification_reason": self.classification_reason,
        }
