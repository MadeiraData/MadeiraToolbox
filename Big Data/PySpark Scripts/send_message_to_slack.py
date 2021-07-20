from slack_sdk import WebClient

#1. create app in Slack
#2. generate Slack access token
#3. Add relevant permissions on Slack channel to the new Slack App

client = WebClient(token="***")

# the below function will generate message in Slack that utilizes Slack block layout
# which contains two fields ( table style) and a button
# when you click the button URL will be opened
def post_to_slack(alert_category, alert_time, jira_url, Slack_channel):
    alert_body = """ [  {{ "color": "#e33a4f",
                                  "fields": [
                                    {{
                                        "title": "Alert Category",
                                        "value": "{}",
                                        "short": true
                                    }},
                                    {{
                                        "title": "Alert Time",
                                        "value": "{}",
                                        "short": true
                                    }}
                                    ]
                                }},
                            {{  "title": "{}",
                                "callback_id": "123",
                                "attachment_type": "default",
                                "actions": [
                                    {{
                                        "name": "open",
                                        "text": "Open Jira Ticket",
                                        "type": "button",
                                        "url": "{}"
                                    }}
                                    ]
                               }}]""".format(alert_category, alert_time, jira_url)
    response = client.chat_postMessage(
            channel=Slack_channel,
            text='*{}*'.format(alert_category),
            attachments=alert_body)

    return response

# response that is returned form this function contains Slack message unique ID response['ts']
# we can use response['ts'] to post additional messages in the conversation thread of that message

if response != "":
    res = client.chat_postMessage(
        channel=SlackChannel,
        thread_ts = response['ts'],
        text='More information in the conversation thread')
    print (res)

