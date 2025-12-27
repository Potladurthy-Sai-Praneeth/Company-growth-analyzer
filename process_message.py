import json
import uuid
import base64
from datetime import datetime

class SlackProcessor:
    """Processes chat messages into Slack API format."""
    def __init__(self):
        pass

    def process(self, row):
        """Processes a row from the dataset and returns a Slack-formatted message."""
        try:
            dt = datetime.strptime(row['timestamp'], "%Y-%m-%d %H:%M:%S")
            ts = f"{dt.timestamp():.6f}"
        except (ValueError, KeyError):
            ts = f"{datetime.now().timestamp():.6f}"

        slack_message = {
            "metadata": {
                "topic": "chat",
                "source": "slack",
                # "company_id": row.get('company_id', 'unknown_company')
            },
            "type": "message",
            "client_msg_id": str(uuid.uuid4()),
            "text": row.get('content', ''),
            "user": row.get('chat_sender', 'U01234567'),
            "ts": ts,
            "team": "T01234567",
            "channel": "C01234567",
            "event_ts": ts,
            "channel_type": "channel",
            "blocks": [
                {
                    "type": "rich_text",
                    "block_id": str(uuid.uuid4())[:8],
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": row.get('content', '')
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        return slack_message

class GmailProcessor:
    """Processes email messages into Gmail API format."""
    def __init__(self):
        pass

    def process(self, row):
        """Processes a row from the dataset and returns a Gmail-formatted message."""
        content = row.get('content', '')
        subject = "No Subject"
        body = content
        
        if content.startswith("Subject: "):
            parts = content.split(". ", 1)
            subject = parts[0].replace("Subject: ", "")
            if len(parts) > 1:
                body = parts[1]
            else:
                body = ""

        encoded_body = base64.urlsafe_b64encode(body.encode('utf-8')).decode('utf-8')

        gmail_message = {
            "metadata": {
                "topic": "email",
                "source": "gmail",
                # "company_id": row.get('company_id', 'unknown_company')
            },
            "id": str(row.get('id', uuid.uuid4().hex[:16])),
            "threadId": str(uuid.uuid4().hex[:16]),
            "labelIds": ["INBOX", "UNREAD"],
            "snippet": body[:100],
            "payload": {
                "partId": "",
                "mimeType": "text/plain",
                "filename": "",
                "headers": [
                    {"name": "Subject", "value": subject},
                    {"name": "From", "value": row.get('email_sender', 'unknown@example.com')},
                    {"name": "Date", "value": row.get('timestamp', datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0000"))}
                ],
                "body": {
                    "size": len(body),
                    "data": encoded_body
                }
            },
            "sizeEstimate": len(body) + 200
        }
        return gmail_message
