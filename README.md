PUT /_template/dm_template
{
  "mappings": {
      "dynamic_templates": [
          {
          "string_fields": {
            "mapping": {
              "type": "keyword"
            },
            "match": "*",
            "match_mapping_type": "string"
          }
        }
      ],
      "properties": {
        "create_time": {
          "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
          "type": "date"
        }, 
        "update_time": {
          "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
          "type": "date"
        }
      }
  },
  "order": 1,
  "settings": {
    "number_of_replicas": 0,
    "number_of_shards": 2
  },
  "index_patterns": "dm*"
}