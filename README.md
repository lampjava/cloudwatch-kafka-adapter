# AWS Cloudwatch

- You need setting Cloudwatch metric for monitoring on AWS Resources.

- Open your integration endpoint 'integration-cloudwatch-kafka'.

- You need setting AWS SNS topic and integration endpoint for subscribing.

### RAW data
~~~
{
  "Type" : "Notification",
  "MessageId" : "3b2f3048-7c8b-5300-b8da-61fc0db82368",
  "TopicArn" : "arn:aws:sns:ap-northeast-2:129335065553:Integration-CloudWatch",
  "Subject" : "ALARM: \"awsec2-i-f4b22853-CPU-Utilization\" in Asia Pacific - Seoul",
  "Message" : "{\"AlarmName\":\"awsec2-i-f4b22853-CPU-Utilization\",\"AlarmDescription\":\"Created from EC2 Console\",\"AWSAccountId\":\"129335065553\",\"NewStateValue\":\"ALARM\",\"NewStateReason\":\"Threshold Crossed: 1 datapoint (42.73) was greater than or equal to the threshold (10.0).\",\"StateChangeTime\":\"2017-06-09T09:48:35.237+0000\",\"Region\":\"Asia Pacific - Seoul\",\"OldStateValue\":\"OK\",\"Trigger\":{\"MetricName\":\"CPUUtilization\",\"Namespace\":\"AWS/EC2\",\"StatisticType\":\"Statistic\",\"Statistic\":\"AVERAGE\",\"Unit\":null,\"Dimensions\":[{\"name\":\"InstanceId\",\"value\":\"i-f4b22853\"}],\"Period\":300,\"EvaluationPeriods\":1,\"ComparisonOperator\":\"GreaterThanOrEqualToThreshold\",\"Threshold\":10.0,\"TreatMissingData\":\"\",\"EvaluateLowSampleCountPercentile\":\"\"}}",
  "Timestamp" : "2017-06-09T09:48:35.306Z",
  "SignatureVersion" : "1",
  "Signature" : "iq0TBOiCAzPSKfJ7hKekvN+APghQp5xm+WQ/+Swb/y2fDSLeN7picwnlh02ZQvmE5bfnF7bR47Puj3HYxfMYJDNLAe6u/FZcgm5+IYdlohWW5Mwg19d/7NNNKR3CyWV2hUNecrx9H29rYYO2EBUsieZuso/qyjt3RpKUapgW/TyasV41iQ9Dwpu/Ky6JQDwK3UcOdzE3eWDYiNZJPBgvTGguDGV+TKtVkGTA6lG33AWqXscrA+dBSOrrfnB1vgkx/TIxYRYijmejqFv2yG++NliVKDB8k1aWI2PyxEpowcadIcmZuIqvK+w4njBurhMVZm0Ee/tKLvpu9+8+DEw6bQ==",
  "SigningCertURL" : "https://sns.ap-northeast-2.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem",
  "UnsubscribeURL" : "https://sns.ap-northeast-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:ap-northeast-2:129335065553:Integration-CloudWatch:190c23ba-fb83-4750-a1f2-1c7cad095b94"
}

~~~

### JSON formatted data
~~~
{
  "Type": "Notification",
  "MessageId": "3b2f3048-7c8b-5300-b8da-61fc0db82368",
  "TopicArn": "arn:aws:sns:ap-northeast-2:129335065553:Integration-CloudWatch",
  "Subject": "ALARM: "awsec2-i-f4b22853-CPU-Utilization" in Asia Pacific - Seoul",
  Message: {
    "AlarmName": "awsec2-i-f4b22853-CPU-Utilization",
    "AlarmDescription": "Created from EC2 Console",
    "AWSAccountId": "129335065553",
    "NewStateValue": "ALARM",
    "NewStateReason": "Threshold Crossed: 1 datapoint (42.73) was greater than or equal to the threshold (10.0).",
    "StateChangeTime": "2017-06-09T09:48:35.237+0000",
    "Region": "Asia Pacific - Seoul",
    "OldStateValue": "OK",
    "Trigger": {
      "MetricName": "CPUUtilization",
      "Namespace": "AWS/EC2",
      "StatisticType": "Statistic",
      "Statistic": "AVERAGE",
      "Unit": null,
      "Dimensions": [
        {
          "name": "InstanceId",
          "value": "i-f4b22853"
        }
      ],
      "Period": 300,
      "EvaluationPeriods": 1,
      "ComparisonOperator": "GreaterThanOrEqualToThreshold",
      "Threshold": 10.0,
      "TreatMissingData": "",
      "EvaluateLowSampleCountPercentile": ""
    }
  },
  "Timestamp": "2017-06-09T09:48:35.306Z",
  "SignatureVersion": "1",
  "Signature": "iq0TBOiCAzPSKfJ7hKekvN+APghQp5xm+WQ/+Swb/y2fDSLeN7picwnlh02ZQvmE5bfnF7bR47Puj3HYxfMYJDNLAe6u/FZcgm5+IYdlohWW5Mwg19d/7NNNKR3CyWV2hUNecrx9H29rYYO2EBUsieZuso/qyjt3RpKUapgW/TyasV41iQ9Dwpu/Ky6JQDwK3UcOdzE3eWDYiNZJPBgvTGguDGV+TKtVkGTA6lG33AWqXscrA+dBSOrrfnB1vgkx/TIxYRYijmejqFv2yG++NliVKDB8k1aWI2PyxEpowcadIcmZuIqvK+w4njBurhMVZm0Ee/tKLvpu9+8+DEw6bQ==",
  "SigningCertURL": "https://sns.ap-northeast-2.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem",
  "UnsubscribeURL": "https://sns.ap-northeast-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:ap-northeast-2:129335065553:Integration-CloudWatch:190c23ba-fb83-4750-a1f2-1c7cad095b94"
}
~~~