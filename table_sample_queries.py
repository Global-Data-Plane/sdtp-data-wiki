table_sample_queries = {
  "samples/nightingale.sdml": [
    {
      "description": "Every record where Disease < 1000",
      "query": '{"operator": "IN_RANGE", "column": "Disease", "min_val": 1000, "max_val": 5000}'
    },
    {
      "description": "Every record for months starting with J",
      "query": '{"operator": "REGEX_MATCH", "column": "Month", "expression": "J*"}'
    },
    {
      "description": "Every record for summer months",
      "query": '{"operator": "IN_LIST", "column": "Month", "values": ["Jun", "Jul", "Aug"]}'
    },
    {
      "description": "Every record for non-summer months",
      "query": '{"operator": "NONE", "arguments":[]"operator": "IN_LIST", "column": "Month", "values": ["Jun", "Jul", "Aug"]}]}'
    }
  
  ]
}