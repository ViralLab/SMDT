# twitter_v1 Data Fromat Sample

```json
{
  "created_at": "Tue Mar 14 20:59:54 +0000 2023",
  "id": ID_1,
  "id_str": "ID_1",
  "text": "@USER1 TEXT",
  "display_text_range": [
    15,
    140
  ],
  "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>",
  "truncated": true,
  "in_reply_to_status_id": ID2,
  "in_reply_to_status_id_str": "ID2",
  "in_reply_to_user_id": ID3,
  "in_reply_to_user_id_str": "ID3",
  "in_reply_to_screen_name": "USER1",
  "user": {
    "id": ID4,
    "id_str": "ID4",
    "name": "👑",
    "screen_name": "USER2",
    "location": null,
    "url": null,
    "description": "ÖDESC1",
    "translator_type": "none",
    "protected": false,
    "verified": false,
    "verified_type": "blue",
    "followers_count": 31,
    "friends_count": 27,
    "listed_count": 0,
    "favourites_count": 589,
    "statuses_count": 4169,
    "created_at": "Fri Nov 22 00:27:16 +0000 2019",
    "utc_offset": null,
    "time_zone": null,
    "geo_enabled": false,
    "lang": null,
    "contributors_enabled": false,
    "is_translator": false,
    "profile_background_color": "F5F8FA",
    "profile_background_image_url": "",
    "profile_background_image_url_https": "",
    "profile_background_tile": false,
    "profile_link_color": "1DA1F2",
    "profile_sidebar_border_color": "C0DEED",
    "profile_sidebar_fill_color": "DDEEF6",
    "profile_text_color": "333333",
    "profile_use_background_image": true,
    "profile_image_url": "URL.jpg",
    "profile_image_url_https": "URL.jpg",
    "default_profile": true,
    "default_profile_image": false,
    "following": null,
    "follow_request_sent": null,
    "notifications": null,
    "withheld_in_countries": []
  },
  "geo": null,
  "coordinates": null,
  "place": null,
  "contributors": null,
  "is_quote_status": false,
  "extended_tweet": {
    "full_text": "@USER1 TEXT",
    "display_text_range": [
      15,
      294
    ],
    "entities": {
      "hashtags": [],
      "urls": [],
      "user_mentions": [
        {
          "screen_name": "USER1",
          "name": "NAME1",
          "id": ID3,
          "id_str": "ID3",
          "indices": [
            0,
            14
          ]
        },
        {
          "screen_name": "USER10",
          "name": "NAME10",
          "id": ID901,
          "id_str": "ID901",
          "indices": [
            68,
            76
          ]
        }
      ],
      "symbols": [],
      "media": [
        {
          "id": ID300,
          "id_str": "ID300",
          "indices": [
            295,
            318
          ],
          "media_url": "URL",
          "media_url_https": "URL",
          "url": "URL",
          "display_url": "URL",
          "expanded_url": "URL",
          "type": "photo",
          "sizes": {
            "thumb": {
              "w": 150,
              "h": 150,
              "resize": "crop"
            },
            "medium": {
              "w": 540,
              "h": 1200,
              "resize": "fit"
            },
            "large": {
              "w": 922,
              "h": 2048,
              "resize": "fit"
            },
            "small": {
              "w": 306,
              "h": 680,
              "resize": "fit"
            }
          }
        }
      ]
    },
    "extended_entities": {
      "media": [
        {
          "id": ID300,
          "id_str": "ID300",
          "indices": [
            295,
            318
          ],
          "media_url": "URL",
          "media_url_https": "URL",
          "url": "URL",
          "display_url": "URL",
          "expanded_url": "URL",
          "type": "photo",
          "sizes": {
            "thumb": {
              "w": 150,
              "h": 150,
              "resize": "crop"
            },
            "medium": {
              "w": 540,
              "h": 1200,
              "resize": "fit"
            },
            "large": {
              "w": 922,
              "h": 2048,
              "resize": "fit"
            },
            "small": {
              "w": 306,
              "h": 680,
              "resize": "fit"
            }
          }
        }
      ]
    }
  },
  "quote_count": 0,
  "reply_count": 0,
  "retweet_count": 0,
  "favorite_count": 0,
  "entities": {
    "hashtags": [],
    "urls": [
      {
        "url": "URL",
        "expanded_url": "URL",
        "display_url": "URL",
        "indices": [
          107,
          130
        ]
      }
    ],
    "user_mentions": [
      {
        "screen_name": "USER1",
        "name": "NAME1",
        "id": ID3,
        "id_str": "ID3",
        "indices": [
          0,
          14
        ]
      },
      {
        "screen_name": "USER10",
        "name": "NAME10",
        "id": ID901,
        "id_str": "ID901",
        "indices": [
          68,
          76
        ]
      },
      {
        "screen_name": "USER1000",
        "name": "NAME12030",
        "id": ID900,
        "id_str": "ID900",
        "indices": [
          89,
          105
        ]
      }
    ],
    "symbols": []
  },
  "favorited": false,
  "retweeted": false,
  "possibly_sensitive": false,
  "filter_level": "low",
  "lang": "tr",
  "timestamp_ms": "1678827594751"
}
```