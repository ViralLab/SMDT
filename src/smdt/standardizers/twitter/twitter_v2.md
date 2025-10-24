# twitter_v2 Data Fromat Sample

```json
{
  "data": {
    "attachments": {},
    "author_id": "id_1",
    "conversation_id": "id_conv_1",
    "created_at": "2023-05-13T21:34:51.000Z",
    "edit_controls": {
      "edits_remaining": 5,
      "is_edit_eligible": false,
      "editable_until": "2023-05-13T22:04:51.000Z"
    },
    "edit_history_tweet_ids": [
      "id_6"
    ],
    "entities": {
      "mentions": [
        {
          "start": 0,
          "end": 12,
          "username": "USER1",
          "id": "id_3"
        }
      ]
    },
    "geo": {},
    "id": "id_6",
    "in_reply_to_user_id": "id_3",
    "lang": "LANG1",
    "possibly_sensitive": false,
    "public_metrics": {
      "retweet_count": 0,
      "reply_count": 0,
      "like_count": 0,
      "quote_count": 0,
      "impression_count": 0
    },
    "referenced_tweets": [
      {
        "type": "replied_to",
        "id": "id_2"
      }
    ],
    "reply_settings": "everyone",
    "text": "@USER1 TEXT"
  },
  "includes": {
    "users": [
      {
        "created_at": "2018-02-17T21:13:38.000Z",
        "description": "",
        "id": "id_1",
        "name": "TEXT",
        "profile_image_url": "TEXT",
        "protected": false,
        "public_metrics": {
          "followers_count": 175,
          "following_count": 234,
          "tweet_count": 935,
          "listed_count": 1
        },
        "username": "USER2",
        "verified": false
      },
      {
        "created_at": "2022-06-15T12:28:08.000Z",
        "description": "TEXT",
        "id": "id_3",
        "name": "TEXT",
        "profile_image_url": "URL",
        "protected": false,
        "public_metrics": {
          "followers_count": 39,
          "following_count": 51,
          "tweet_count": 1121,
          "listed_count": 0
        },
        "username": "USER1",
        "verified": false
      }
    ],
    "tweets": [
      {
        "attachments": {},
        "author_id": "id_1",
        "conversation_id": "id_conv_1",
        "created_at": "2023-05-13T21:34:51.000Z",
        "edit_controls": {
          "edits_remaining": 5,
          "is_edit_eligible": false,
          "editable_until": "2023-05-13T22:04:51.000Z"
        },
        "edit_history_tweet_ids": [
          "id_6"
        ],
        "entities": {
          "mentions": [
            {
              "start": 0,
              "end": 12,
              "username": "USER1",
              "id": "id_3"
            }
          ]
        },
        "geo": {},
        "id": "id_6",
        "in_reply_to_user_id": "id_3",
        "lang": "LANG1",
        "possibly_sensitive": false,
        "public_metrics": {
          "retweet_count": 0,
          "reply_count": 0,
          "like_count": 0,
          "quote_count": 0,
          "impression_count": 0
        },
        "referenced_tweets": [
          {
            "type": "replied_to",
            "id": "id_2"
          }
        ],
        "reply_settings": "everyone",
        "text": "@USER1 TEXT"
      },
      {
        "attachments": {},
        "author_id": "id_3",
        "conversation_id": "id_conv_1",
        "created_at": "2023-05-13T21:30:21.000Z",
        "edit_controls": {
          "edits_remaining": 5,
          "is_edit_eligible": false,
          "editable_until": "2023-05-13T22:00:21.000Z"
        },
        "edit_history_tweet_ids": [
          "id_2"
        ],
        "entities": {
          "mentions": [
            {
              "start": 0,
              "end": 14,
              "username": "USER2",
              "id": "id_1"
            }
          ]
        },
        "geo": {},
        "id": "id_2",
        "in_reply_to_user_id": "id_1",
        "lang": "LANG1",
        "possibly_sensitive": false,
        "public_meLANG1ics": {
          "retweet_count": 0,
          "reply_count": 1,
          "like_count": 1,
          "quote_count": 0,
          "impression_count": 12
        },
        "referenced_tweets": [
          {
            "type": "replied_to",
            "id": "id_conv_1"
          }
        ],
        "reply_settings": "everyone",
        "text": "@USER2 TEXT"
      }
    ]
  },
  "matching_rules": [
    {
      "id": "Id_10",
      "tag": "LANG1-tweets"
    }
  ],
  "__twarc": {
    "url": "URL",
    "version": "2.13.0",
    "retrieved_at": "2023-05-13T21:34:57+00:00"
  }
}
```